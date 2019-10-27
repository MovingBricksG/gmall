package com.atguigu.gmall0513.realtime.util

import java.util

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyESUtil {

    private val ES_HOST = "http://gch102"
    private val ES_HTTP_PORT = 9200
    private var factory: JestClientFactory = null

    def getClient: JestClient = {
        if (factory == null) {
            build()
        }
        factory.getObject
    }

    def close(client: JestClient): Unit = {
        if (client != null) try
            client.shutdownClient()
        catch {
            case e: Exception =>
                e.printStackTrace()
        }
    }

    private def build(): Unit = {
        factory = new JestClientFactory
        // KafkaConsumer is not safe for multi-threaded access
        factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
                .maxTotalConnection(20) // 连接总数
                .connTimeout(10000).readTimeout(10000).build)

    }

    def insertBulk(sourceList: List[(String, Any)], indexName: String, typeName: String): Unit = {
        val jest: JestClient = getClient
        val bulkBuilder = new Bulk.Builder()
        bulkBuilder.defaultIndex(indexName).defaultType(typeName)

        for ((id, source) <- sourceList) {
            val index: Index = new Index.Builder(source).id(id).build() //代表一次插入动作
            bulkBuilder.addAction(index)
        }

        val bulk: Bulk = bulkBuilder.build()
        val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulk).getItems
        println(s" 保存= ${items.size()} 条数据!")
    }

    case class Stud( name:String, gender:String)

    def main(args: Array[String]): Unit = {
        val jest: JestClient = getClient
        val stud4 = Stud("li4","male")
        val index: Index = new Index.Builder(stud4).index("gmall0513_stud").`type`("stud").id("2").build() //代表一次插入动作
        jest.execute(index)
    }
}
