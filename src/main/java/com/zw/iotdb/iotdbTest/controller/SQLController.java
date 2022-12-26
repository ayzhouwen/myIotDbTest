package com.zw.iotdb.iotdbTest.controller;

import com.zw.iotdb.iotdbTest.common.entity.AjaxResult;
import com.zw.iotdb.iotdbTest.common.util.IotDbUtil;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 *
 * 基本的sql操作
 * @author
 */
@RestController
@RequestMapping("/iotdb/sql")
public class SQLController {
    @Resource
    private IotDbUtil iotDbUtil;

    @PostMapping("createDataBase")
    public AjaxResult createDataBase(String name) throws InterruptedException {
        if (iotDbUtil.createDataBase(name)){
            return AjaxResult.success();
        }else {
            return AjaxResult.error("创建数据库异常");
        }
    }
}
