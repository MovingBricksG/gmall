package com.atguigu.gmall.publisher.gmall0513_publisher.service;

import java.util.Map;

public interface PublisherService {

    public int getDauTotal(String date );

    public Map getDauHours(String date );

    public Double getOrderAmount(String date);

    public Map getOrderAmountHour(String date);
}
