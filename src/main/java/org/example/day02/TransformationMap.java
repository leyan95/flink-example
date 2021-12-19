package org.example.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wujianchuan 2021/12/19
 * @version 1.0
 */
public class TransformationMap {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> numberStream = environment.fromElements(1, 2, 3, 4, 5);
        SingleOutputStreamOperator<Integer> doubleNumberStream = numberStream.map((MapFunction<Integer, Integer>) number -> number * 2).returns(Types.INT);
        SingleOutputStreamOperator<String> stringDoubleNumberStream = doubleNumberStream.map(new RichMapFunction<Integer, String>() {
            // open 在构造方法之后，map方法执行之前，执行一次，Configuration可以拿到全局配置
            // 通常用来初始化连接，或者初始化或恢复state
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("open");
            }

            @Override
            public String map(Integer integer) throws Exception {
                return "number - " + integer;
            }

            // 销毁之前，执行一次，通常做资源的释放
            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("close");
            }
        });
        stringDoubleNumberStream.print();
        environment.execute("TransformationMap");
    }
}
