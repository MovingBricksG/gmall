package com.atguigu.gmall.publisher.gmall0513_publisher.service.impl;

import com.atguigu.gmall.publisher.gmall0513_publisher.mapper.DauMapper;
import com.atguigu.gmall.publisher.gmall0513_publisher.mapper.OrderMapper;
import com.atguigu.gmall.publisher.gmall0513_publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper daoMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public int getDauTotal(String date) {
        return daoMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHours(String date) {
        Map<String, Long> map = new HashMap<>();
        List<Map> hourList = daoMapper.selectDauTotalHourMap(date);
        for (Map hour : hourList) {
            map.put((String) hour.get("LH"), (Long) hour.get("CT"));
        }
        return map;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        Map<String, Double> map = new HashMap<>();
        List<Map> hourList = orderMapper.selectOrderAmountHourMap(date);
        for (Map hour : hourList) {
            map.put((String) hour.get("CH"), (Double) hour.get("SA"));
        }
        return map;
    }
}
