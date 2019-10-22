package com.atguigu.gmall.publisher.gmall0513_publisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.publisher.gmall0513_publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("/realTimeTotal")
    public String realTimeTotal(@RequestParam("date") String date) {

        List<Map> list = new ArrayList<>();
        // 获取当日数据总量
        int dauTotal = publisherService.getDauTotal(date);
        Map<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        // 获取当日新增用户总数
        HashMap<String, Object> midMap = new HashMap<>();

        midMap.put("id", "new_mid");
        midMap.put("name", "新增设备");
        midMap.put("value", 323);

        // 获取当日新增订单
        HashMap<String, Object> orderMap = new HashMap<>();
        orderMap.put("id", "order_amount");
        orderMap.put("name", "新增交易额");
        orderMap.put("value", publisherService.getOrderAmount(date));

        list.add(dauMap);
        list.add(midMap);
        list.add(orderMap);
        return JSON.toJSONString(list);
    }

    @GetMapping("/realTimeHours")
    public String realTimeHours(@RequestParam("id") String id, @RequestParam("date") String date) {

        if ("dau".equals(id)) {
            // 获取今天的 小时-用户数
            Map dauHoursToday = publisherService.getDauHours(date);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("today", dauHoursToday);

            // 获取昨天的 小时-用户数
            String yesterDay = "";
            try {
                Date todayDate = new SimpleDateFormat("yyyy-MM-dd").parse(date);
                Date yesterdatDate = DateUtils.addDays(todayDate, -1);
                yesterDay = new SimpleDateFormat("yyyy-MM-dd").format(yesterdatDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            Map dauHoursYesterday = publisherService.getDauHours(yesterDay);
            jsonObject.put("yesterday", dauHoursYesterday);
            return jsonObject.toJSONString();
        } else if ("order_amount".equals(id)) {
            JSONObject jsonObject = new JSONObject();
            Map orderHourTMap = publisherService.getOrderAmountHour(date);
            String yesterDay = "";
            try {
                Date todayDate = new SimpleDateFormat("yyyy-MM-dd").parse(date);
                Date yesterdatDate = DateUtils.addDays(todayDate, -1);
                yesterDay = new SimpleDateFormat("yyyy-MM-dd").format(yesterdatDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            Map orderHourYMap = publisherService.getOrderAmountHour(yesterDay);
            jsonObject.put("yesterday", orderHourYMap);
            jsonObject.put("today", orderHourTMap);
            return jsonObject.toJSONString();
        }
        return null;
    }


}
