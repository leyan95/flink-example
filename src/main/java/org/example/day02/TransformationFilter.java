package org.example.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wujianchuan 2021/12/19
 * @version 1.0
 */
public class TransformationFilter {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> numberStream = environment.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);
        SingleOutputStreamOperator<Integer> oddStream = numberStream.filter((FilterFunction<Integer>) integer -> integer % 2 != 0).returns(Types.INT);
        oddStream.print();
        environment.execute("TransformationFilter");
    }
}
