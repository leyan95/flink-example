package org.example.day02;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author wujianchuan 2021/12/18
 * @version 1.0
 */
public class KafkaWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("topic001")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> words = environment.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .flatMap((FlatMapFunction<String, String>) (line, collector) -> {
                    String[] lineWords = line.split(" ");
                    for (String word : lineWords) {
                        collector.collect(word);
                    }
                }).returns(Types.STRING);

        DataStream<Tuple2<String, Integer>> wordAndOne = words.map((MapFunction<String, Tuple2<String, Integer>>) word -> Tuple2.of(word, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        DataStream<Tuple2<String, Integer>> sum = wordAndOne.keyBy(tp -> tp.f0).sum(1);

        sum.print();

        environment.execute("KafkaSource");
    }
}
