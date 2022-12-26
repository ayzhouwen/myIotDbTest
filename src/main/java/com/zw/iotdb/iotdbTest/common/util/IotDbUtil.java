package com.zw.iotdb.iotdbTest.common.util;

import cn.hutool.core.convert.Convert;
import com.zw.iotdb.iotdbTest.common.config.AppConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 操作iotdb util工具
 */

@Component
@Slf4j
public class IotDbUtil {
    private  SessionPool pool;
    @Resource
    private AppConfig appConfig;
    @PostConstruct
    void  init(){
        pool  =new SessionPool.Builder().host(appConfig.getIotdbHost())
                .port(Convert.toInt(appConfig.getIotdbPort())).user(appConfig.getIotdbUser())
                .password(appConfig.getIotdbPassword()).maxSize(12).build();
    }

    /**
     * 创建数据库
     * @param name
     * @return
     */
    public boolean createDataBase(String name)  {
        try {
            pool.createDatabase(name);
            return true;
        } catch (Exception e) {
            log.error("创建iotdb数据库:{}异常:",name,e);
            return false;
        }
    }
    public  List<Map<String,String>> querySql(String sql) {
        List<Map<String,String>> rowList=new ArrayList<>();
        SessionDataSetWrapper wrapper = null;
        try {
            wrapper = pool.executeQueryStatement(sql);
            // get DataIterator like JDBC
            SessionDataSet.DataIterator dataIterator = wrapper.iterator();
            while (dataIterator.next()) {
                Map<String,String> row=new HashMap<>();
                for (String columnName : wrapper.getColumnNames()) {
                    row.put(columnName,dataIterator.getString(columnName));
                }
                rowList.add(row);
            }
        } catch (Exception e) {
           log.error("执行查询:{}异常:",sql,e);
        } finally {
            // remember to close data set finally!
            pool.closeResultSet(wrapper);
        }
        return rowList;
    }

}
