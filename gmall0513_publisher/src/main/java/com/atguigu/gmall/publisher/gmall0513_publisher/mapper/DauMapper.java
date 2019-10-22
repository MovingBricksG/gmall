package com.atguigu.gmall.publisher.gmall0513_publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {

    Integer selectDauTotal(String date);
    List<Map> selectDauTotalHourMap(String date);
}
