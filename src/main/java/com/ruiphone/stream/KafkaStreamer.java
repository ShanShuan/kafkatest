package com.ruiphone.stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * Description:
 *
 * @author wang zifeng
 * @Date Create on 2019-07-25 10:11
 * @since version1.0 Copyright 2018 Burcent All Rights Reserved.
 */
@Slf4j
public class KafkaStreamer {
    public static void main(String[] args) {
        //创建拓扑 对象
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.addSource("soure","hello");
        topologyBuilder.addProcessor("processor", new ProcessorSupplier(){

            @Override
            public Processor get() {
                return new LogProcessor();
            }
        }, "soure");
        topologyBuilder.addSink("sink","world","processor");
        //创建配置
        Properties props=new Properties();
        props.put("application.id","KafkaStreamer");
        props.put("bootstrap.servers","192.168.157.128:9092");
        KafkaStreams kafkaStreams = new KafkaStreams(topologyBuilder, props);
        kafkaStreams.start();
    }
}
