package com.atguigu.gmall0513.dwlogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.constant.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class LogController {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/log")
    public String test(@RequestParam("logString")String logMessage) {

        // 补充时间戳
        JSONObject jsonObject = JSON.parseObject(logMessage);
        jsonObject.put("ts",System.currentTimeMillis());

        // 落盘
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);

        // 推送到kafka
        if( "startup".equals( jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,jsonString);
        }else{
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,jsonString);
        }
        // System.out.println(logMessage);
        return "hello demo";
    }
}
