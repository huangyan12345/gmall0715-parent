package com.atguigu.realtime.gmall0715logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.atguigu.gmall0715.common.constant.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController //就相当与Controller+ResponseBody
@Slf4j
public class LogController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @PostMapping("log")
    public String log(@RequestParam("logString")String logString){
    //1.加时间戳
     JSONObject jsonObject =  JSON.parseObject(logString);
     jsonObject.put("ts",System.currentTimeMillis());
     //2.落盘成文件
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);
        //发送kafka
        //先分流
        if("startup".equals(jsonObject.getString("type"))){

            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonString);
        }else {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonString);

        }
        return "success";
    }
}
