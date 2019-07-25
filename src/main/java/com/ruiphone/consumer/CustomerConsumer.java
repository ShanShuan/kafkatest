package com.ruiphone.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Description: 高级api
 *
 * @author wang zifeng
 * @Date Create on 2019-07-24 17:21
 * @since version1.0 Copyright 2018 Burcent All Rights Reserved.
 */
public class CustomerConsumer {


    public static void main(String[] args) {
        Properties props = new Properties();
        //kafka 集群
      props.put("bootstrap.servers", "192.168.157.128:9092");
      //消费者组信息
      props.put("group.id", "test");
      //是否自动提交  （提交offset）
      props.put("enable.auto.commit", "false");
      //提交延时
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        //KV  反序列化
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      //创建 消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        //指定topic
        kafkaConsumer.subscribe(Arrays.asList("hello","world"));
        while(true) {
            //获取数据
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(100);
            //TODO 真正的业务处理
            for (ConsumerRecord<String, String> one : poll) {
                System.out.println("topic:" + one.topic() + "---partitions:" + one.partition() + "----value:" + one.value() + "-----key:" + one.key());
            }
        }
    }
}
