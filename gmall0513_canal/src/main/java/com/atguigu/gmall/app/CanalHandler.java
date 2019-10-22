package com.atguigu.gmall.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.constant.GmallConstants;
import com.atguigu.gmall.util.MyKafkaSender;

import java.util.List;

public class CanalHandler {

    CanalEntry.EventType eventType;

    String tableName;

    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDatasList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDatasList;
    }

    public void handle() {
        if (tableName.equals("order_info") && eventType == CanalEntry.EventType.INSERT) {
            sendRowDataToKafka(GmallConstants.KAFKA_TOPIC_ORDER);
        } else if (tableName.equals("user_info") && eventType == CanalEntry.EventType.INSERT) {
            sendRowDataToKafka(GmallConstants.KAFKA_TOPIC_USER);
        }
    }

    private void sendRowDataToKafka(String topic) {
        for (CanalEntry.RowData rowData : rowDataList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                String name = column.getName();
                String value = column.getValue();
                System.out.println(name + "::" + value);
                jsonObject.put(name, value);
            }
            String rowJson = jsonObject.toJSONString();
            MyKafkaSender.send(topic, rowJson);
        }
    }
}
