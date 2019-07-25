package com.ruiphone.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Description: 自定义 partitioner
 *
 * @author wang zifeng
 * @Date Create on 2019-07-24 17:11
 * @since version1.0 Copyright 2018 Burcent All Rights Reserved.
 */
public class CustomerPartitioner implements Partitioner {

    private  Map configs;

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 0;
    }


    public void close() {

    }

    /**
     * 可以获取配置文件
     * @param configs
     */
    public void configure(Map<String, ?> configs) {
        this.configs=configs;
    }
}
