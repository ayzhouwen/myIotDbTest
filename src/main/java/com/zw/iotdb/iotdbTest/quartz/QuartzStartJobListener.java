package com.zw.iotdb.iotdbTest.quartz;

import com.zw.iotdb.iotdbTest.service.PerformanceTestService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;

import javax.annotation.Resource;

/**
 * 初始化开始调度任务
 * 
 * @author
 *
 */
@Configuration
@Slf4j
public class QuartzStartJobListener implements ApplicationListener<ContextRefreshedEvent> {
	@Resource
	private PerformanceTestService performanceTestService;
	/**
	 * 项目启动后操作
	 */
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		log.info("开始执行写入性能测试###################################");
		performanceTestService.insertMTabletTest();
		log.info("结束执行写入性能测试###################################");
	}



}
