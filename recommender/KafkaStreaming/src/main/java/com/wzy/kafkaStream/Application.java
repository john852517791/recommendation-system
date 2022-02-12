package com.wzy.kafkaStream;/*
 *@Auther: wzy
 *@Date:2021/5/9 -05-09
 *@Descreption: com.wzy.kafkaStream
 *@Version: 1.0
 */

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class Application {

    public static void main(String[] args) {
        String brokers = "localhost:9092";
        String zookeepers = "localhost:2181";

//        定义输入和输出的kafka topic
        String from="log";
        String to = "recommender";

//        定义kafka stream的配置参数
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

//        创建kafka stream 配置对象
        StreamsConfig config = new StreamsConfig(settings);

//        定义拓扑构建器, 从from再到top再输出
        TopologyBuilder Builder = new TopologyBuilder();

        Builder.addSource("SOURCE",from)
                .addProcessor("PROCESSOR",()->new LogProcessor(),"SOURCE")
                .addSink("SINK",to,"PROCESSOR");

//        创建kafka stream
        KafkaStreams streams = new KafkaStreams(Builder, config);
//        开启kafka stream
        streams.start();
        System.out.println("kafka stream has started>>>>>>>>>>>>>>>>>");


    }
}
