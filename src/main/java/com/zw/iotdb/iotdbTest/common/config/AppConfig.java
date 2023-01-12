package com.zw.iotdb.iotdbTest.common.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class AppConfig {
    //iot连接配置项
    @Value("${iotdb.host}")
    private String iotdbHost;
    @Value("${iotdb.port}")
    private String iotdbPort;
    @Value("${iotdb.user}")
    private String iotdbUser;
    @Value("${iotdb.password}")
    private String iotdbPassword;
    //iot性能测试配置
    //写入设备编号后缀,不同压力机需要设置不同,否则影响测试统计
    @Value("${iotdbTest.deviceCodeSuffix}")
    private String deviceCodeSuffix;
    //测试写入多少轮
    @Value("${iotdbTest.writeWheel}")
    private String writeWheel;
    //每轮写入多少数据
    @Value("${iotdbTest.allWriteRowNum}")
    private String allWriteRowNum;
    //测试写入时是否启用批量写,1启用,0:不启用(单条写入)
    @Value("${iotdbTest.openBatchWrite}")
    private String openBatchWrite;
    //一个批写入多少条数据 默认1000
    @Value("${iotdbTest.batchWriteRowNum}")
    private String batchWriteRowNum;
    @Value("${iotdbTest.writeThreadNum}")
    //并发写入线程数
    private String writeThreadNum;
}
