package com.atguigu.gmall0513.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.constant.GmallConstants
import com.atguigu.gmall0513.realtime.bean.StartUpLog
import com.atguigu.gmall0513.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("dau_app").setMaster("local[*]")

        val ssc = new StreamingContext(conf, Seconds(5))

        val startUpStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

        /*startUpStream.foreachRDD{rdd => {
            println(rdd.map(_.value()).collect().mkString("\n"))
        }}*/
        // 转换格式 同时补充两个时间字段
        val startupMapStream = startUpStream.map { record => {
            val jsonString = record.value()
            val startUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
            val formatter = new SimpleDateFormat("yyyy-MM-dd HH")
            val dateHour = formatter.format(new Date(startUpLog.ts))
            val dateHourArr = dateHour.split(" ")
            startUpLog.logDate = dateHourArr(0)
            startUpLog.logHour = dateHourArr(1)
            startUpLog
        }
        }

        // 进行过滤
        // 使用广播变量
        val filteredStream = startupMapStream.transform(rdd => {
            // driver 每5s执行一次
            println("过滤前：" + rdd.count())
            val client = RedisUtil.getJedisClient
            val dateString = "dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            val dauSet = client.smembers(dateString)
            val dauBroadcast = ssc.sparkContext.broadcast(dauSet)
            val filteredRdd = rdd.filter(startUpLog => { // executor
                val dauMidSet = dauBroadcast.value
                !dauMidSet.contains(startUpLog.mid)
            })
            println("过滤后：" + filteredRdd.count())
            filteredRdd
        })


        // 批次内去重
        val groupStream: DStream[(String, Iterable[StartUpLog])] = filteredStream.map(log => (log.mid, log)).groupByKey()
        val uniqueStream = groupStream.flatMap {
            case (k, v) => {
                val topList = v.toList.sortWith { (s1, s2) => {
                    s1.ts < s2.ts
                }
                }.take(1)
                topList
            }
        }

        // 将访问清单保存到redis
        uniqueStream.foreachRDD { rdd =>
            rdd.foreachPartition(startUpItr => {
                //val jedisClient = new Jedis("gch102", 6379)
                val client = RedisUtil.getJedisClient
                for (stratUp <- startUpItr) {
                    val dauKey = "dau:" + stratUp.logDate
                    client.sadd(dauKey, stratUp.mid)
                }

                client.close()
            })
            /*rdd.foreach(startUpLog => {
                // 保存redis操作
                val jedisClient = new Jedis("gch102", 6379)
                val dauKey = "dau:" + startUpLog.logDate
                jedisClient.sadd(dauKey,  startUpLog.mid)
                jedisClient.close()
            })*/
        }

        ssc.start()
        ssc.awaitTermination()
    }
}
