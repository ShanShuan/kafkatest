package com.ruiphone.producer.intercetor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Description:
 *
 * @author wang zifeng
 * @Date Create on 2019-07-25 09:56
 * @since version1.0 Copyright 2018 Burcent All Rights Reserved.
 */
public class CountInterceptor implements ProducerInterceptor<String,String> {

    private int successCount=0;
    private int errorCount=0;
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception!=null){
            successCount++;
        }else{
            errorCount++;
        }

    }

    @Override
    public void close() {
        System.out.println("发送成功："+successCount+"条数据");
        System.out.println("发送失败："+errorCount+"条数据");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
