package com.zw.iotdb.iotdbTest;

import cn.hutool.json.JSONUtil;
import com.zw.iotdb.iotdbTest.common.util.IotDbUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

@SpringBootTest(classes= IotdbTestApplication.class,webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Slf4j
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
		log.info(JSONUtil.toJsonStr(iotDbUtil.querySql("show databases")));
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
