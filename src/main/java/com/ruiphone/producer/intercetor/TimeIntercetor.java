package com.ruiphone.producer.intercetor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Description:
 *
 * @author wang zifeng
 * @Date Create on 2019-07-25 09:50
 * @since version1.0 Copyright 2018 Burcent All Rights Reserved.
 */
public class TimeIntercetor implements ProducerInterceptor<String,String> {

    @Override
    public ProducerRecord<String,String> onSend(ProducerRecord record) {
        return (ProducerRecord<String,String>) new ProducerRecord(record.topic(),record.partition(),record.key(),System.currentTimeMillis()+","+record.value());
    }
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
