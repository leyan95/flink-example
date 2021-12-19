package org.example.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author wujianchuan 2021/12/19
 * @version 1.0
 */
public class TransformationFlatMap {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> linesStream = environment.fromElements("flink spark hadoop", "java scala flink kafka");
        SingleOutputStreamOperator<String> wordsStream = linesStream.flatMap((FlatMapFunction<String, String>) (s, collector) -> Arrays.stream(s.split(" ")).forEach(collector::collect)).returns(Types.STRING);
        wordsStream.print();
        environment.execute("TransformationFlatMap");
    }
}
