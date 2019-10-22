package com.atguigu.gmall.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalApp {

    public static void main(String[] args) {

        // 1.连接canal服务器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("gch102", 11111),
                "example",
                "",
                "");

        // 2.抓取数据
        while (true) {
            canalConnector.connect();
            canalConnector.subscribe("gmall0513.*");
            // 一次抓取多个message
            Message message = canalConnector.get(100);
            List<CanalEntry.Entry> entries = message.getEntries();
            // 3.抓取数据后 提取数据
            // 当没有数据时 减少频繁的访问
            if (entries.size() == 0) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                // 一个entry就是一个sql执行的结果集
                for (CanalEntry.Entry entry : entries) {
                    // 如果是业务数据
                    if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange = null;
                        try {
                            //反序列化工具
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        String tableName = entry.getHeader().getTableName();
                        // 4.交给handler处理
                        CanalHandler canalHandler = new CanalHandler(rowChange.getEventType(), tableName, rowDatasList);
                        canalHandler.handle();
                    }
                }
            }
        }
    }
}
