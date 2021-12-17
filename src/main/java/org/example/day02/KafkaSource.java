package org.example.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;


/**
 * @author wujianchuan 2021/12/18
 * @version 1.0
 */
public class KafkaSource {

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        // 指定Kafka的Broker地址
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // 指定组ID
        properties.setProperty("group.id", "flink-learn");
        // 如果没有记录偏移量，第一次从最开始消费
        properties.setProperty("auto.offset.reset", "earliest");
        // kafka的消费者不自动提交偏移量
        // properties.setProperty("enable.auto.conmmit", "false");

        // KafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("topic001", new SimpleStringSchema(), properties);
        // Source
        DataStream<String> lines = environment.addSource(kafkaSource, Types.STRING);
        // Sink
        lines.print();

        environment.execute("KafkaSource");
    }
}
