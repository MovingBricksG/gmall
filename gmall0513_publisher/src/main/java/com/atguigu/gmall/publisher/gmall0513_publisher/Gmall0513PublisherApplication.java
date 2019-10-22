package com.atguigu.gmall.publisher.gmall0513_publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmall.publisher.gmall0513_publisher.mapper")
public class Gmall0513PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0513PublisherApplication.class, args);
    }

}
