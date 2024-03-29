package com.zw.iotdb.iotdbTest.service;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.PageUtil;
import com.zw.iotdb.iotdbTest.common.config.AppConfig;
import com.zw.iotdb.iotdbTest.common.util.IotDbUtil;
import com.zw.iotdb.iotdbTest.common.util.MyDateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 性能测试，实验结果，
 * 0.iotdb中同一个物理量中默认每秒最大写入是1000条（写多了会覆盖，也就是丢数据，一般生产环境不会出现），除非把时间戳设置为纳秒 timestamp_precision=ns
 * 1.由于顺序写，一般批量写 非纳秒写入，机械能接近固态的速度（大概 机械25多万，固态33万）
 * 2.多线程提升性能不是很明显
 * 3.如果设置纳秒测试，固态能达到每秒百万（也许还能提升），垃圾机械20万，但是固态下会造成时间戳格式化展示失效
 * 4.生产环境insertMTabletTest是符合实际数据的（每秒8万左右，因为数据上报是一个时间的所有测试，不是一个时间段的所有数据），
 * 5.iotdb的性能跟服务器硬件，和上报数据格式，和监测点数量和批量写入数量都有关系，得实际压测，没有一个标准答案
 *
 *
 */
@Service
@Slf4j
public class PerformanceTestService {
    @Resource
    private IotDbUtil iotDbUtil;
    @Resource
    private AppConfig appConfig;

    //时间戳纳秒测试,IOTDB timestamp_precision需要配置成ns，单写入固态能达到每秒万条，批量写固态能达到每秒百万左右
    // 垃圾机械硬盘能达到每秒23万左右
    public void NsTimeWriteTest() {
        //
        List<String> measurements = new ArrayList<>(Arrays.asList("pv", "ps"));
        int threadNum = Convert.toInt(appConfig.getWriteThreadNum());
        ThreadPoolExecutor poolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNum);
        int openBatchWrite = Convert.toInt(appConfig.getOpenBatchWrite());
        int allWriteRowNum = Convert.toInt(appConfig.getAllWriteRowNum());
        int batchWriteRowNum = Convert.toInt(appConfig.getBatchWriteRowNum());
        int writeWheel = Convert.toInt(appConfig.getWriteWheel());

        if (writeWheel < 1) {
            log.error("写入总轮数至少为1");
            return;
        }
        //判断批次写入时,总数据量是否小于单个批次
        if (openBatchWrite == 1 && allWriteRowNum < batchWriteRowNum) {
            log.error("已经开启批量写入模式,总数量不能小于批次写入数据量");
            return;
        }

        //每秒平均写入
        String deviceCode = "root.jmdb.s1.d" + appConfig.getDeviceCodeSuffix()+"_";;
        for (int j = 0; j < writeWheel; j++) {
            double averageWrite = 0;
            long stime = System.currentTimeMillis();
            CountDownLatch countDownLatch = null;
            if (openBatchWrite == 0) {
                countDownLatch = new CountDownLatch(allWriteRowNum);
                List<TSDataType> dataTypeList = Arrays.asList(TSDataType.DOUBLE, TSDataType.INT32);
                //数据写入
                for (int i = 0; i < allWriteRowNum; i++) {
                    //监控主机号
                    int finalI = i;
                    CountDownLatch finalCountDownLatch = countDownLatch;
                    poolExecutor.execute(() -> {
                        try {
                            long timestamp = System.currentTimeMillis() * 1000000L;
                            long offset = finalI % 1000000L;
                            timestamp = timestamp + offset;
                            iotDbUtil.pool.insertRecord(deviceCode,
                                    timestamp, measurements, dataTypeList, Arrays.asList(Convert.toDouble(finalI), finalI % 4));
//                          log.info("成功写入第{}条数据:",finalI);
                        } catch (Exception e) {
                            log.error("写入数据异常,当前写入条数:{}", finalI, e);
                            log.info(MyDateUtil.execTime("iotdb写入测试", stime));
                        } finally {
                            finalCountDownLatch.countDown();
                        }
                    });

                }
                try {
                    countDownLatch.await();
                } catch (Exception e) {
                    log.error("等待写入测试异常");
                }
                Double etime = Convert.toDouble(DateUtil.spendMs(stime));

                log.info(MyDateUtil.execTime("iotdb多线程个数:" + threadNum + ",写入条数" + allWriteRowNum, stime));
                averageWrite = allWriteRowNum / etime * 1000;
                log.info("第{}轮平均每秒写入数据量:{}", j + 1, NumberUtil.decimalFormat("#", averageWrite));
            } else if (openBatchWrite == 1) {

                if (threadNum!=1){
                    log.error("单设备纳秒批量写只能设置为1个线程");
                    return;
                }
                int totalPage = PageUtil.totalPage(allWriteRowNum, batchWriteRowNum);
                countDownLatch = new CountDownLatch(totalPage);
                List<MeasurementSchema> schemaList = new ArrayList<>();
                schemaList.add(new MeasurementSchema("pv", TSDataType.DOUBLE));
                schemaList.add(new MeasurementSchema("ps", TSDataType.INT32));

                //数据写入
                for (int i = 0; i < totalPage; i++) {
                    int finalI = i;
                    CountDownLatch finalCountDownLatch = countDownLatch;
                    //本页生成数据数量计算
                    long generateNum = batchWriteRowNum;
                    if ((i + 1) == totalPage) {
                        int n = (i + 1) * batchWriteRowNum - allWriteRowNum;
                        if (n > 0) {
                            generateNum = batchWriteRowNum - n;
                        }
                    }
                    long finalGenerateNum = generateNum;

                        try {
                            Tablet tablet = new Tablet(deviceCode, schemaList, batchWriteRowNum);

                            for (long row = 0; row < finalGenerateNum; row++) {
                                int rowIndex = tablet.rowSize++;
                                //纳秒偏移量
                                long timestamp = System.currentTimeMillis() * 1000000L;
                                long offset = finalI * batchWriteRowNum + row % 1000000L;
                                timestamp = timestamp + offset;
                                tablet.addTimestamp(rowIndex, timestamp);
                                tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, Convert.toDouble(offset));
                                tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, Convert.toInt(row % 4));
                                if (tablet.rowSize == tablet.getMaxRowNumber()) {
                                    iotDbUtil.pool.insertTablet(tablet, true);
                                    tablet.reset();
                                }

                            }

                            if (tablet.rowSize != 0) {
                                iotDbUtil.pool.insertTablet(tablet);
                                tablet.reset();
                            }
                        } catch (Exception e) {
                            log.error("写入数据异常,当前写入页:{}", finalI, e);
                            log.info(MyDateUtil.execTime("iotdb写入测试", stime));
                        } finally {
                            finalCountDownLatch.countDown();
                        }

//                    try {
//                        Thread.sleep(0);
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    }

                }
                try {
                    countDownLatch.await();
                } catch (Exception e) {
                    log.error("等待写入测试异常");
                }
                Double etime = Convert.toDouble(DateUtil.spendMs(stime));

                log.info(MyDateUtil.execTime("iotdb多线程个数:" + threadNum + ",写入条数" + allWriteRowNum, stime));
                averageWrite = allWriteRowNum / etime * 1000;
                log.info("第{}轮平均每秒写入数据量:{}", j + 1, NumberUtil.decimalFormat("#", averageWrite));

            } else {
                log.error("是否开启批量写入测试错误");
                return;
            }
        }


    }

    /**
     * 单线程多设备insertTablet写入，IOTDB timestamp_precision=ms即可，目前只支持批量写入
     *  iotdb多线程个数:1,写入条数1000000:1743.0毫秒,1.74秒,0.03分钟
     */
    public void insertTabletTest() {
        int threadNum = Convert.toInt(appConfig.getWriteThreadNum());
        int openBatchWrite = Convert.toInt(appConfig.getOpenBatchWrite());
        int allWriteRowNum = Convert.toInt(appConfig.getAllWriteRowNum());
        int batchWriteRowNum = Convert.toInt(appConfig.getBatchWriteRowNum());
        int writeWheel = Convert.toInt(appConfig.getWriteWheel());
        String deviceCodePrefix = "root.jmdb.s1.d_" + appConfig.getDeviceCodeSuffix()+"_"
                +"110116000000_0204_000088_00040002_0001_";
        if (writeWheel < 1) {
            log.error("写入总轮数至少为1");
            return;
        }
        if (openBatchWrite != 1) {
            log.error("该测试模块不支持单条写入");
            return;
        }
        //判断批次写入时,总数据量是否小于单个批次
        if (openBatchWrite == 1 && allWriteRowNum < batchWriteRowNum) {
            log.error("已经开启批量写入模式,总数量不能小于批次写入数据量");
            return;
        }
        if (batchWriteRowNum>1000){
            log.error("该测试模块批量写入最大1000条");
            return;
        }

        if (threadNum!=1){
            log.error("多设备批量写只能设置为1个线程");
            return;
        }

        for (int j = 0; j < writeWheel; j++) {
            double averageWrite = 0;
            long stime = System.currentTimeMillis();
            int totalPage = PageUtil.totalPage(allWriteRowNum, batchWriteRowNum);
            List<MeasurementSchema> schemaList = new ArrayList<>();
            schemaList.add(new MeasurementSchema("pv", TSDataType.DOUBLE));
            schemaList.add(new MeasurementSchema("ps", TSDataType.INT32));
            //数据写入
            for (int i = 0; i < totalPage; i++) {
                //本页生成数据数量计算
                long generateNum = batchWriteRowNum;
                if ((i + 1) == totalPage) {
                    int n = (i + 1) * batchWriteRowNum - allWriteRowNum;
                    if (n > 0) {
                        generateNum = batchWriteRowNum - n;
                    }
                }

                try {
                    Tablet tablet = new Tablet(deviceCodePrefix+i, schemaList, batchWriteRowNum);
                    //毫秒偏移量
                    long timestamp = System.currentTimeMillis() ;
                    for (long row = 0; row < generateNum; row++) {
                        int rowIndex = tablet.rowSize++;
                        long offset = timestamp+row;
                        tablet.addTimestamp(rowIndex, offset);
                        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, Convert.toDouble(offset));
                        tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, Convert.toInt(row % 4));
                        if (tablet.rowSize == tablet.getMaxRowNumber()) {
                            iotDbUtil.pool.insertTablet(tablet, true);
                            tablet.reset();
                        }

                    }
                    if (tablet.rowSize != 0) {
                        iotDbUtil.pool.insertTablet(tablet);
                        tablet.reset();
                    }
                } catch (Exception e) {
                    log.error("写入数据异常,当前写入页:{}", i, e);
                    log.info(MyDateUtil.execTime("iotdb写入测试", stime));
                } finally {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                }

            }
            Double etime = Convert.toDouble(DateUtil.spendMs(stime));
            log.info(MyDateUtil.execTime("iotdb多线程个数:" + threadNum + ",写入条数" + allWriteRowNum, stime));
            averageWrite = allWriteRowNum / etime * 1000;
            log.info("第{}轮平均每秒写入数据量:{}", j + 1, NumberUtil.decimalFormat("#", averageWrite));

        }
    }

    /**
     * 单线程多设备insertTablets写入，IOTDB timestamp_precision=ms即可，目前只支持批量写入
     * 固态每秒8万左右（注意模拟的真实生产环境50万测点数据，table的列表长度其实是1个元素，接近真实数据，真实数据不可能给你批量报上来，都是一个时间点的数据）
     */
    public void insertMTabletTest() {
        int threadNum = Convert.toInt(appConfig.getWriteThreadNum());
        int openBatchWrite = Convert.toInt(appConfig.getOpenBatchWrite());
        int allWriteRowNum = Convert.toInt(appConfig.getAllWriteRowNum());
        int batchWriteRowNum = Convert.toInt(appConfig.getBatchWriteRowNum());
        int writeWheel = Convert.toInt(appConfig.getWriteWheel());
        String deviceCodePrefix = "root.jmdb.s1.d_" + appConfig.getDeviceCodeSuffix()+"_"
                +"110116000000_0204_000088_00040002_0001_";
        if (writeWheel < 1) {
            log.error("写入总轮数至少为1");
            return;
        }
        if (openBatchWrite != 1) {
            log.error("该测试模块不支持单条写入");
            return;
        }
        //判断批次写入时,总数据量是否小于单个批次
        if (openBatchWrite == 1 && allWriteRowNum < batchWriteRowNum) {
            log.error("已经开启批量写入模式,总数量不能小于批次写入数据量");
            return;
        }

        if (batchWriteRowNum>1000){
            log.error("该测试模块批量写入最大1000条");
            return;
        }

        if (threadNum!=1){
            log.error("多设备批量写只能设置为1个线程");
            return;
        }

        for (int j = 0; j < writeWheel; j++) {
            double averageWrite = 0;
            long stime = System.currentTimeMillis();
            int totalPage = PageUtil.totalPage(allWriteRowNum, batchWriteRowNum);
            List<MeasurementSchema> schemaList = new ArrayList<>();
            schemaList.add(new MeasurementSchema("pv", TSDataType.DOUBLE));
            schemaList.add(new MeasurementSchema("ps", TSDataType.INT32));
            //数据写入
            for (int i = 0; i < totalPage; i++) {
                //本页生成数据数量计算
                long pageStartTime=System.currentTimeMillis();
                long generateNum = batchWriteRowNum;
                if ((i + 1) == totalPage) {
                    int n = (i + 1) * batchWriteRowNum - allWriteRowNum;
                    if (n > 0) {
                        generateNum = batchWriteRowNum - n;
                    }
                }
                try {
                    Map<String, Tablet> tabletsMap=new HashMap<>();
                    //毫秒偏移量
                    long timestamp = System.currentTimeMillis() ;
                    for (long row = 0; row < generateNum; row++) {
                        long ln=i*batchWriteRowNum+row;
                        //控制设备（jm中叫监测点）数量，设备数量越少，相对写入速度越快
                        ln=ln%10000*50;
                        Tablet tablet = new Tablet(deviceCodePrefix+ln, schemaList, batchWriteRowNum);
                        int rowIndex = tablet.rowSize++;
                        long offset = timestamp+row;
                        tablet.addTimestamp(rowIndex, offset);
                        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, Convert.toDouble(offset));
                        tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, Convert.toInt(row % 4));
                        tabletsMap.put(deviceCodePrefix+ln,tablet);
                    }
                    iotDbUtil.pool.insertTablets(tabletsMap,true);
                    tabletsMap.forEach((k,v)->{
                        v.reset();
                    });
                } catch (Exception e) {
                    log.error("写入数据异常,当前写入页:{}", i, e);
                    log.info(MyDateUtil.execTime("iotdb写入测试", stime));
                } finally {
//                    try {
//                        Thread.sleep(1);
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    }

                }
                log.info(MyDateUtil.execTime("本次批量写入条数" + generateNum+",页码："+i, pageStartTime));
            }
            Double etime = Convert.toDouble(DateUtil.spendMs(stime));
            log.info(MyDateUtil.execTime("iotdb多线程个数:" + threadNum + ",写入条数" + allWriteRowNum, stime));
            averageWrite = allWriteRowNum / etime * 1000;
            log.info("第{}轮平均每秒写入数据量:{}", j + 1, NumberUtil.decimalFormat("#", averageWrite));

        }
    }


}
