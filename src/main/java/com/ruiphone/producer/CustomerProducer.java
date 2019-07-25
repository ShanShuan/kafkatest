package com.ruiphone.producer;


import org.apache.kafka.clients.producer.*;

import java.util.Arrays;
import java.util.Properties;

/**
 * Description:
 *
 * @author wang zifeng
 * @Date Create on 2019-07-24 16:08
 * @since version1.0 Copyright 2018 Burcent All Rights Reserved.
 */
public class CustomerProducer {


    public static void main(String[] args) {
          Properties props = new Properties();
          //kafka集群
          props.put("bootstrap.servers", "192.168.157.128:9092");
          //ack 异拿机制
          props.put("acks", "all");
          //重试次数
          props.put("retries", 0);
          //批量大小（数据达到多少 再发送  不是每一次 来一个发送一个）
          props.put("batch.size", 16384);
          //提交延时（如果没有达到批量的大小 超过这个时间 也发送）
          props.put("linger.ms", 1);
          //缓存
          props.put("buffer.memory", 33554432);
          //KV  的 序列化类
          props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
          props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

          //拦截器
          props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Arrays.asList("com.ruiphone.producer.intercetor.TimeIntercetor","com.ruiphone.producer.intercetor.CountInterceptor"));
          //自定义分区
//          props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.ruiphone.producer.CustomerPartitioner");
          //创建生成者对象
          KafkaProducer<String, String> stringStringKafkaProducer = new KafkaProducer<>(props);
            for (int i = 0; i <9 ; i++) {
                System.out.println(i+";;");
                stringStringKafkaProducer.send(new ProducerRecord<String, String>("hello", String.valueOf(i)), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception==null){
                            System.out.println("partition:"+metadata.partition()+"-- offset:"+metadata.offset());
                        }else{
                            System.out.println("发送失败");
                        }
                    }
                });
            }
          stringStringKafkaProducer.close();
    }




}
