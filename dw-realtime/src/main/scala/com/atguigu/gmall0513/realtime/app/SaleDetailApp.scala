package com.atguigu.gmall0513.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.constant.GmallConstants
import com.atguigu.gmall0513.realtime.bean.{OrderDetail, OrderInfo}
import com.atguigu.gmall0513.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SaleDetailApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(5))

        val orderDetailInputDStream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)
        val orderInputDStream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

        // 对两个流进行转换 转为bean
        val orderDStream = orderInputDStream.map { rdd => {
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
        val orderDetailDStream =orderDetailInputDStream.map{ record => {
            val jsonString = record.value()
            val detail = JSON.parseObject(jsonString, classOf[OrderDetail])
            detail
        }}

        // 对两个流转换为 id->bean的形式
        val orderWithKeyDStream = orderDStream.map(orderInfo => {(orderInfo.id, orderInfo)})
        val orderDetailWithKeyDStream = orderDetailDStream.map(orderDetail => {(orderDetail.order_id, orderDetail)})

        //  join
        val joinDStream = orderWithKeyDStream.join(orderDetailWithKeyDStream)



    }
}
