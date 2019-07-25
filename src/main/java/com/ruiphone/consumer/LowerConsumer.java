package com.ruiphone.consumer;

import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Description: 低级消费 api
 * 根据指定的topic  Partition  offset来获取数据
 *
 * @author wang zifeng
 * @Date Create on 2019-07-24 19:42
 * @since version1.0 Copyright 2018 Burcent All Rights Reserved.
 */
public class LowerConsumer {
    public static void main(String[] args) {
        //定义相关参数
        //kafka 集群
        ArrayList<String> brokers=new ArrayList<String>();
        brokers.add("192.168.157.128");
        //端口号
         int port=9092;
         //主题
        String topic="hello";
        //分区
        int partition=0;

        //偏移量
        long offset=0;


        LowerConsumer lowerConsumer=new LowerConsumer();
        lowerConsumer.getData(brokers,topic,port,partition,offset);

    }

    /**
     * 找分区领导
     * @return
     */
    public BrokerEndPoint findLeader(List<String> brokes, String topic, int port, int partition){
        for (String broke : brokes) {
            //创建获取分区leader的消费对象
            SimpleConsumer getleader = new SimpleConsumer(broke, port, 1000, 4 * 1024, "getLeader");
            //创建一个主题元数据信息请求
            TopicMetadataRequest request=new TopicMetadataRequest(Arrays.asList(topic));
            //获取主题元数据返回值
            TopicMetadataResponse topicMetadataResponse = getleader.send(request);
            //解析主题元数据
            List<TopicMetadata> topicMetadata = topicMetadataResponse.topicsMetadata();
            for (TopicMetadata topicMetadatum : topicMetadata) {
                //获取多个 分区元数据
                List<PartitionMetadata> partitionMetadatas = topicMetadatum.partitionsMetadata();
                //遍历分区元数据
                for (PartitionMetadata partitionMetadata : partitionMetadatas) {
                    if(partitionMetadata.partitionId()==partition){

                        return partitionMetadata.leader();
                    }

                }
            }
        }

        return null;
    }


    /**
     * 获取数据
     */
    private void getData(List<String> brokes, String topic, int port, int partition,long offset){
        BrokerEndPoint leader = findLeader(brokes, topic, port, partition);
        if(leader==null){
            return ;
        }

        String host = leader.host();
        //获取数据的消费对象
        SimpleConsumer getDate = new SimpleConsumer(host, port, 1000, 4 * 1024, "getDate");
        //创建获取数据的对象 （fetchSize  字节数）
        kafka.api.FetchRequest build = new FetchRequestBuilder().addFetch(topic, partition, offset, 500000).build();
        //获取数据返回值
        FetchResponse fetch = getDate.fetch(build);
        //解析返回值
        ByteBufferMessageSet messageAndOffsets = fetch.messageSet(topic, partition);
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
           long offset1= messageAndOffset.offset();
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes=new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(offset1+"--返回message："+bytes);
        }

    }
}
