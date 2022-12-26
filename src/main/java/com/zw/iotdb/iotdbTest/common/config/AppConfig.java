package com.zw.iotdb.iotdbTest.common.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class AppConfig {
    //iotDB 相关配置
    @Value("${iotdb.host}")
    private String iotdbHost;
    @Value("${iotdb.port}")
    private String iotdbPort;
    @Value("${iotdb.user}")
    private String iotdbUser;
    @Value("${iotdb.password}")
    private String iotdbPassword;
}
