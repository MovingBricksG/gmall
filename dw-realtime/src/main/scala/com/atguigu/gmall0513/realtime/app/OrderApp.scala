package com.atguigu.gmall0513.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.constant.GmallConstants
import com.atguigu.gmall0513.realtime.bean.OrderInfo
import com.atguigu.gmall0513.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object OrderApp {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("order_info").setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(5))

        val inputStream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

        val mapStream = inputStream.map { rdd => {
            val jsonString = rdd.value()
            val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
            val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
            // 脱敏
            orderInfo.create_date = createTimeArr(0)
            orderInfo.create_hour = createTimeArr(1).split(":")(0)
            val tel3_8: (String, String) = orderInfo.consignee_tel.splitAt(3)
            val front3: String = tel3_8._1 //138****1234
            val back4: String = tel3_8._2.splitAt(4)._2
            orderInfo.consignee_tel = front3 + "****" + back4
            orderInfo
        }}
        mapStream.foreachRDD{ rdd => {
            val configuration = new Configuration()
            println(rdd.collect().mkString("\n"))
            rdd.saveToPhoenix("GMALL2019_ORDER_INFO",
                Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS",
                    "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS",
                    "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY",
                    "CREATE_DATE", "CREATE_HOUR"),
                configuration,
                Some("gch102,gch103,gch104:2181"))

        }}

        ssc.start()
        ssc.awaitTermination()
    }
}
