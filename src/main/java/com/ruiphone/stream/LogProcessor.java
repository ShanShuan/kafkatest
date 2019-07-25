package com.ruiphone.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Description: 处理流
 *
 * @author wang zifeng
 * @Date Create on 2019-07-25 11:14
 * @since version1.0 Copyright 2018 Burcent All Rights Reserved.
 */
public class LogProcessor implements Processor<byte[],byte[]>{
    ProcessorContext processorContext;

    @Override
    public void init(ProcessorContext processorContext) {
       this.processorContext=processorContext;
    }

    @Override
    public void process(byte[] bytes, byte[] bytes2) {
        System.out.println("1111111");
        String line=new String(bytes);
        line=line.replaceAll(">>>","");
        System.out.println(line);
        bytes2 = line.getBytes();
        processorContext.forward(bytes,bytes2);
    }

    @Override
    public void punctuate(long l) {
        System.out.println("444");
    }

    @Override
    public void close() {

    }



}
