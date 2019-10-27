package com.atguigu.gmall0513.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.logging.SimpleFormatter

import com.alibaba.fastjson.JSON
import com.atguigu.constant.GmallConstants
import com.atguigu.gmall0513.realtime.bean.{CouponAlertInfo, EventInfo}
import com.atguigu.gmall0513.realtime.util.{MyESUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

object CouponAlertApp {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("coupon")
        conf.set("spark.streaming.kafka.consumer.cache.enabled", "false")
        val ssc = new StreamingContext(conf, Seconds(5))

        val inputDStream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

        // 1.调整结构
        val mapDStream = inputDStream.map { record => {
            val jsonString = record.value()
            val eventInfo = JSON.parseObject(jsonString, classOf[EventInfo])
            val format = new SimpleDateFormat("yyyy-MM-dd HH")
            val dates = format.format(new Date(eventInfo.ts)).split(" ")
            eventInfo.logDate = dates(0)
            eventInfo.logHour = dates(1)
            eventInfo
        }
        }
        // 2.开窗
        val windowDStream: DStream[EventInfo] = mapDStream.window(Seconds(300), Seconds(10))
        // 3.分组
        val groupDStream = windowDStream.map(record => {
            (record.mid, record)
        }).groupByKey()
        // 4.筛选
        //    a 三次几以上领取优惠券   //在组内根据mid的事件集合进行 过滤筛选
        //    b 用不同账号
        //    c  在过程中没有浏览商品
        val filteredDStream = groupDStream.map {
            case (mid, eventItr) => {
                // 领取优惠券登录过的uid
                val couponUidsSet = new util.HashSet[String]()
                // 优惠券涉及的商品id
                val itemIdsSet = new util.HashSet[String]()
                val eventIds = new util.ArrayList[String]()
                // 是否为优惠券
                var isCoupon: Boolean = true
                breakable(
                    for (event <- eventItr.toList) {
                        eventIds.add(event.evid)
                        if (event.evid == "coupon") {
                            itemIdsSet.add(event.itemid)
                            couponUidsSet.add(event.uid)
                        }  else if (event.evid == "clickItem") {
                            isCoupon = false
                            break()
                        }
                    }
                )
                (isCoupon && couponUidsSet.size() >= 3,
                        CouponAlertInfo(mid, couponUidsSet, itemIdsSet, eventIds, System.currentTimeMillis()))
            }
        }
//        filteredDStream.foreachRDD{ rdd => {
//            println(rdd.collect().mkString("\n"))
//        }}
        val transDStream = filteredDStream.filter(_._1).map { case (flag, alertInfo) => {
            val period = alertInfo.ts / 1000l / 60l
            val id = alertInfo.mid + "_" + period
            (id, alertInfo)
        }
        }

        // 在es中去重.
        transDStream.foreachRDD(rdd => {
            rdd.foreachPartition{
                alertInfoWithIdItr => {
                    MyESUtil.insertBulk(alertInfoWithIdItr.toList, GmallConstants.ES_INDEX_ALERT, GmallConstants.ES_DEFAULT_TYPE)
                }
            }
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
