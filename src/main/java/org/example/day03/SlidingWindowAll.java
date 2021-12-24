package org.example.day03;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 滑动窗口
 *
 * @author wujianchuan 2021/12/24
 * @version 1.0
 */
public class SlidingWindowAll {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = environment.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Integer> numbers = lines.map(Integer::parseInt);
        AllWindowedStream<Integer, TimeWindow> window = numbers.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));
        SingleOutputStreamOperator<Integer> summed = window.sum(0);
        summed.print();
        environment.execute("CountWindowAll");
    }
}
