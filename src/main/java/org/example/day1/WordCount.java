package org.example.day1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author 吴建川
 * @version 0.0.1
 * @date 2021/12/14
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> lines = executionEnvironment.socketTextStream("localhost", 8888);
        final DataStream<Tuple2<String, Integer>> countStream = lines.flatMap(
                (FlatMapFunction<String, Tuple2<String, Integer>>) (line, collector) -> Arrays.asList(line.split(" "))
                        .forEach(word -> {
                            Tuple2<String, Integer> tp = Tuple2.of(word, 1);
                            collector.collect(tp);
                        })
        ).returns(Types.TUPLE(Types.STRING, Types.INT));
        DataStream<Tuple2<String, Integer>> sum = countStream.keyBy(0).sum(1);
        sum.print().setParallelism(1);
        executionEnvironment.execute("StreamWordCount");
    }
}
