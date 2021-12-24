package org.example.day03;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 分组后再调用CountWindow，每个组达到指定条数才会触发任务
 *
 * @author wujianchuan 2021/12/24
 * @version 1.0
 */
public class CountWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // spark,3
        // hadoop,2
        DataStreamSource<String> lines = environment.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(line -> {
            String[] strings = line.split(",");
            return Tuple2.of(strings[0], Integer.parseInt(strings[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));
        // 先分组，再划分窗口
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndCount.keyBy(t -> t.f0);
        // 划分窗口
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> window = keyed.countWindow(5);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = window.sum(1);
        sum.print();
        environment.execute("CountWindowAll");
    }
}
