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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 性能测试
 */
@Service
@Slf4j
public class PerformanceTestService {
    @Resource
    private IotDbUtil iotDbUtil;
    @Resource
    private AppConfig appConfig;

    //多线程写入测试,IOTDB timestamp_precision需要配置成ns
    public   void multiThreadWriteTest()  {
        //
        List<String> measurements= new ArrayList<>(Arrays.asList("pv","ps"));
        int threadNum=Convert.toInt(appConfig.getWriteThreadNum());
        ThreadPoolExecutor poolExecutor= (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNum);
        int openBatchWrite=Convert.toInt(appConfig.getOpenBatchWrite());
        int allWriteRowNum=Convert.toInt(appConfig.getAllWriteRowNum());
        int batchWriteRowNum=Convert.toInt(appConfig.getBatchWriteRowNum());
        int writeWheel=Convert.toInt(appConfig.getWriteWheel());

        if (writeWheel<1){
            log.error("写入总轮数至少为1");
            return;
        }
        //判断批次写入时,总数据量是否小于单个批次
        if (openBatchWrite==1&&allWriteRowNum<batchWriteRowNum){
            log.error("已经开启批量写入模式,总数量不能小于批次写入数据量");
            return;
        }

        //每秒平均写入
        String deviceCode="root.jmdb.s1.d"+appConfig.getDeviceCodeSuffix();
        for (int j = 0; j < writeWheel; j++) {
            double averageWrite=0;
            long stime=System.currentTimeMillis();
            CountDownLatch countDownLatch=null;
            if (openBatchWrite==0){
                countDownLatch=new CountDownLatch(allWriteRowNum);
                List<TSDataType> dataTypeList=Arrays.asList(TSDataType.DOUBLE,TSDataType.INT32);
                //数据写入
                for (int i = 0; i <allWriteRowNum ; i++) {
                    //监控主机号
                    int finalI = i;
                    CountDownLatch finalCountDownLatch = countDownLatch;
                    poolExecutor.execute(()->{
                        try {
                            long timestamp = System.currentTimeMillis()*1000000L;
                            long offset=finalI%1000000L;
                            timestamp=timestamp+offset;
                            iotDbUtil.pool.insertRecord(deviceCode,
                                    timestamp,measurements,dataTypeList,Arrays.asList(Convert.toDouble(finalI) , finalI %4));
//                          log.info("成功写入第{}条数据:",finalI);
                        } catch (Exception e) {
                            log.error("写入数据异常,当前写入条数:{}", finalI,e);
                            log.info(MyDateUtil.execTime("iotdb写入测试",stime));
                        }finally {
                            finalCountDownLatch.countDown();
                        }
                    });

                }
                try {
                    countDownLatch.await();
                } catch (Exception e) {
                    log.error("等待写入测试异常");
                }
                Double etime= Convert.toDouble(DateUtil.spendMs(stime));

                log.info(MyDateUtil.execTime("iotdb多线程个数:"+threadNum+",写入条数"+allWriteRowNum,stime));
                averageWrite=allWriteRowNum/etime*1000;
                log.info("第{}轮平均每秒写入数据量:{}",j+1,NumberUtil.decimalFormat("#",averageWrite));
            }else if (openBatchWrite==1){
                int totalPage=PageUtil.totalPage(allWriteRowNum,batchWriteRowNum);
                countDownLatch=new CountDownLatch(totalPage);
                List<MeasurementSchema> schemaList = new ArrayList<>();
                schemaList.add(new MeasurementSchema("pv", TSDataType.DOUBLE));
                schemaList.add(new MeasurementSchema("ps", TSDataType.INT32));

                //数据写入
                for (int i = 0; i <totalPage ; i++) {
                    int finalI = i;
                    CountDownLatch finalCountDownLatch = countDownLatch;
                    //本页生成数据数量计算
                    long generateNum=batchWriteRowNum;
                    if ((i+1)==totalPage){
                        int n=(i+1)*batchWriteRowNum-allWriteRowNum;
                        if (n>0){
                            generateNum=batchWriteRowNum-n;
                        }
                    }
                    long finalGenerateNum = generateNum;
                    poolExecutor.execute(()->{
                        try {
                            Tablet tablet = new Tablet(deviceCode, schemaList, batchWriteRowNum);

                            for (long row = 0; row < finalGenerateNum; row++) {
                                int rowIndex = tablet.rowSize++;
                                //纳秒偏移量
                                long timestamp = System.currentTimeMillis()*1000000L;
                                long offset=finalI*batchWriteRowNum+row%1000000L;
                                timestamp=timestamp+offset;
                                tablet.addTimestamp(rowIndex, timestamp);
                                tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, Convert.toDouble(offset));
                                tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, Convert.toInt(row%4));
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
                            log.error("写入数据异常,当前写入页:{}", finalI,e);
                            log.info(MyDateUtil.execTime("iotdb写入测试",stime));
                        }finally {
                            finalCountDownLatch.countDown();
                        }
                    });
                    try {
                        Thread.sleep(0);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                }
                try {
                    countDownLatch.await();
                } catch (Exception e) {
                    log.error("等待写入测试异常");
                }
                Double etime= Convert.toDouble(DateUtil.spendMs(stime));

                log.info(MyDateUtil.execTime("iotdb多线程个数:"+threadNum+",写入条数"+allWriteRowNum,stime));
                averageWrite=allWriteRowNum/etime*1000;
                log.info("第{}轮平均每秒写入数据量:{}",j+1,NumberUtil.decimalFormat("#",averageWrite));

            }else{
                log.error("是否开启批量写入测试错误");
                return;
            }
        }


    }
}
