package org.example.day03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * 滚动窗口
 * 分组后再调用CountWindow，每个组达到指定条数才会触发任务
 * 如果使用并行的source，例如kafkasource，创建kafka的topic时有多个分区
 * 每一个source的分区都要满足触发的条件，整个窗口才会被触发
 *
 * @author wujianchuan 2021/12/24
 * @version 1.0
 */
public class EventTimeTumblingWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<String> strategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((element, timestamp) -> Long.parseLong(element.split(",")[0]));

        // spark,3
        // hadoop,2
        SingleOutputStreamOperator<String> lines = environment.socketTextStream("localhost", 8888).assignTimestampsAndWatermarks(strategy);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(line -> {
            String[] strings = line.split(",");
            return Tuple2.of(strings[1], Integer.parseInt(strings[2]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));
        // 先分组，再划分窗口
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndCount.keyBy(t -> t.f0);
        // 划分窗口
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyed.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = window.sum(1);
        sum.print();
        environment.execute("CountWindowAll");
    }
}
