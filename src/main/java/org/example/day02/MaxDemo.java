package org.example.day02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wujianchuan 2021/12/22
 * @version 1.0
 */
public class MaxDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = executionEnvironment.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndNum = lines.map(line -> {
            String[] words = line.split(",");
            return Tuple2.of(words[0], Integer.parseInt(words[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndNum.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> max = keyed.max(1);
        max.print();
        executionEnvironment.execute("MaxDemo");
    }
}
