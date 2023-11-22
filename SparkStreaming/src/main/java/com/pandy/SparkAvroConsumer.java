package com.pandy;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class SparkAvroConsumer {

    public static void main(String[] args) throws InterruptedException {
        // 初始化 Spark Streaming Context
        JavaStreamingContext ssc = new JavaStreamingContext("local[2]", "SparkAvroConsumer", Durations.seconds(1));

        // Kafka 相关配置
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "hadoop102:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "mygroup");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // Kafka 主题
        Collection<String> topics = Collections.singletonList("mytopic");

        // 创建 Kafka DStream
        JavaInputDStream<ConsumerRecord<String, String>> directKafkaStream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        directKafkaStream.foreachRDD(rdd -> {
            rdd.foreach(avroRecord -> {
                Schema.Parser parser = new Schema.Parser();
                Schema schema = parser.parse(SimpleAvroProducer.USER_SCHEMA);
//                Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
//                GenericRecord record = recordInjection.invert(avroRecord.value().getBytes(StandardCharsets.UTF_8)).get();

                GenericRecord deserializedUser = AvroDSerialization.deserializeAvroRecord(avroRecord.value().getBytes(StandardCharsets.UTF_8), schema);

                System.out.println("str1= " + deserializedUser.get("str1")
                        + ", str2= " + deserializedUser.get("str2")
                        + ", int1=" + deserializedUser.get("int1"));
            });
        });

//        // 处理 Kafka 消息
//        directKafkaStream.map(record -> record.value()).print();

        // 启动 Spark Streaming
        ssc.start();
        ssc.awaitTermination();
    }
}
