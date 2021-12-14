package org.example.day1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 吴建川
 * @version 0.0.1
 * @date 2021/12/14
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> lines = executionEnvironment.socketTextStream("localhost", 8888);

        DataStream<String> words = lines.flatMap((FlatMapFunction<String, String>) (line, collector) -> {
            String[] words1 = line.split(" ");
            for (String word : words1) {
                collector.collect(word);
            }
        }).returns(Types.STRING);

        DataStream<Tuple2<String, Integer>> wordAndOne = words.map((MapFunction<String, Tuple2<String, Integer>>) word -> Tuple2.of(word, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        DataStream<Tuple2<String, Integer>> sum = wordAndOne.keyBy(tp -> tp.f0).sum(1);
        sum.print();
        executionEnvironment.execute("StreamWordCount");
    }
}
