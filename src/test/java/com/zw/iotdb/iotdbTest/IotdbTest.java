package com.zw.iotdb.iotdbTest;

import cn.hutool.core.convert.Convert;
import cn.hutool.json.JSONUtil;
import com.zw.iotdb.iotdbTest.common.util.IotDbUtil;
import com.zw.iotdb.iotdbTest.common.util.MyDateUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

@SpringBootTest(classes= IotdbTestApplication.class,webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Slf4j
/**
 * 注意压力测试的时候,不要执行DELETE DATABASE root.** ,否则会严重影响性能
 */
class IotdbTest {
	@Resource
	private IotDbUtil iotDbUtil;
	@Test
	void createDataBase(){
		iotDbUtil.createDataBase("root.zw1");
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void query(){
		log.info(JSONUtil.toJsonStr(iotDbUtil.querySql(" SELECT temperature as temperature FROM root.zwdb.wf01.wt01")));
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void noQuerySQL(){
		iotDbUtil.execSql("CREATE DATABASE root.zw2");
		iotDbUtil.execSql("CREATE DATABASE root.zw3");
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 单线程写入压力测试
	 * IOTDB以单机版运行在vmware虚拟机中 ,虚拟机配置:cnetos7.5虚拟机,4核8线程,16g内存,50g固态
	 * IOTDB以单机版运行在 物理笔记本配置: win10,cpu i5-9300H ,内存32g,磁盘 256固态
	 * 虚拟机测试结果: 1线程,跑10万条数据,跑完全都是57秒左右,平均每秒写入1754行,虚拟机内存加到32g,有点效果但不是很明显
	 * 物理机测试结果:1线程,跑10万条数据,跑完全都是18秒左右,平均每条写入5555行
	 */
	@Test
	void singleThreadWriteTest(){
		//删除数据库
		List<String>measurements= new ArrayList<>(Arrays.asList("pointVaule","pointState"));
		long stime=System.currentTimeMillis();
		//数据写入
		for (int i = 0; i <100000 ; i++) {
			//监控主机号
			int suCodeSuffix=i%5000;
			int deviceCodeSuffix=i%20;

			try {
				iotDbUtil.pool.insertRecord("root.jmdb.D010200100000E"+suCodeSuffix+"."+"M11010700500000001030300000E"+deviceCodeSuffix,
						System.currentTimeMillis(),measurements,Arrays.asList(i+"", Convert.toStr(i%10)));
			} catch (Exception e) {
				log.error("写入数据异常,当前写入条数:{}",i,e);
				log.info(MyDateUtil.execTime("iotdb写入测试",stime));
				return;
			}
			log.info("成功写入第{}条数据:",i);
		}

		log.info(MyDateUtil.execTime("iotdb写入测试",stime));

	}


	/**
	 * 多线程写入压力测试
	 * IOTDB以单机版运行在vmware虚拟机中 ,虚拟机配置:cnetos7.5虚拟机,4核8线程,16g内存,50g固态
	 * IOTDB以单机版运行在 物理笔记本配置: win10,cpu i5-9300H ,内存32g,磁盘 256固态
	 * 虚拟机测试结果: 8线程和16线程1024线程,跑10万条数据,全跑完都是12秒左右,平均每秒写入8333行,增大线程数和连接数调和虚拟机内存加到32g,有点效果但不是很明显
	 * 物理机测试结果:8线程和16线程,1024线程,跑10万条数据,全跑完都是7秒左右,平均每条写入14285行, 增大线程数和连接数,有点效果,但是不是很明显
	 */
	@Test
	void multiThreadWriteTest() throws InterruptedException {
		//删除数据库
		List<String>measurements= new ArrayList<>(Arrays.asList("pointVaule","pointState"));
        int threadNum=16;
		ThreadPoolExecutor poolExecutor= (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNum);
		int rowSize=10000000;
		CountDownLatch countDownLatch=new CountDownLatch(rowSize);
		long stime=System.currentTimeMillis();
		//数据写入
		for (int i = 0; i <rowSize ; i++) {
			//监控主机号
			int suCodeSuffix=i%5000;
			int deviceCodeSuffix=i%20;
			int finalI = i;
			poolExecutor.execute(()->{
				try {
					iotDbUtil.pool.insertRecord("root.jmdb.D010200100000E"+suCodeSuffix+"."+"M11010700500000001030300000E"+deviceCodeSuffix,
							System.currentTimeMillis(),measurements,Arrays.asList(finalI +"", Convert.toStr(finalI %10)));
					log.info("成功写入第{}条数据:",finalI);
				} catch (Exception e) {
					log.error("写入数据异常,当前写入条数:{}", finalI,e);
					log.info(MyDateUtil.execTime("iotdb写入测试",stime));
				}finally {
					countDownLatch.countDown();
				}
			});

		}
		countDownLatch.await();
		log.info(MyDateUtil.execTime("iotdb多线程个数:"+threadNum+",写入条数:"+rowSize+":",stime));

	}



}
