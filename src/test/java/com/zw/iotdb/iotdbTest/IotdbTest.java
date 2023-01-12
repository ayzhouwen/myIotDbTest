package com.zw.iotdb.iotdbTest;

import cn.hutool.core.convert.Convert;
import cn.hutool.json.JSONUtil;
import com.zw.iotdb.iotdbTest.common.util.IotDbUtil;
import com.zw.iotdb.iotdbTest.common.util.MyDateUtil;
import com.zw.iotdb.iotdbTest.service.PerformanceTestService;
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

	@Resource
	private PerformanceTestService performanceTestService;
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





	@Test
	void multiThreadWriteTest() throws InterruptedException {
		performanceTestService.multiThreadWriteTest();
	}



}
