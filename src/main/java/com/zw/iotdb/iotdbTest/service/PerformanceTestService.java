package com.zw.iotdb.iotdbTest.service;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.NumberUtil;
import com.zw.iotdb.iotdbTest.common.config.AppConfig;
import com.zw.iotdb.iotdbTest.common.util.IotDbUtil;
import com.zw.iotdb.iotdbTest.common.util.MyDateUtil;
import lombok.extern.slf4j.Slf4j;
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

    //多线程写入测试
  public   void multiThreadWriteTest(){
        //删除数据库
        List<String> measurements= new ArrayList<>(Arrays.asList("pointVaule","pointState"));
        int threadNum=Runtime.getRuntime().availableProcessors()*2;
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
      for (int j = 0; j < writeWheel; j++) {
          double averageWrite=0;
          long stime=System.currentTimeMillis();
          CountDownLatch countDownLatch=null;
          if (openBatchWrite==0){
              countDownLatch=new CountDownLatch(allWriteRowNum);
              //数据写入
              for (int i = 0; i <allWriteRowNum ; i++) {
                  //监控主机号
                  int suCodeSuffix=1;
                  int deviceCodeSuffix=i%20;
                  int finalI = i;
                  CountDownLatch finalCountDownLatch = countDownLatch;
                  poolExecutor.execute(()->{
                      try {
                          iotDbUtil.pool.insertRecord("root.jmdb.D010200100000E"+suCodeSuffix+"."+"M11010700500000001030300000E"+deviceCodeSuffix,
                                  System.currentTimeMillis(),measurements,Arrays.asList(finalI +"", Convert.toStr(finalI %10)));
                          log.debug("成功写入第{}条数据:",finalI);
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
              int average=1;

          }else{
              log.error("是否开启批量写入测试错误");
              return;
          }
      }


    }
}
