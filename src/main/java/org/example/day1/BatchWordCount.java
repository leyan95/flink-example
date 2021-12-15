package org.example.day1;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author 吴建川
 * @version 0.0.1
 * @date 2021/12/15
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<String> lines = environment.readTextFile("E:\\a.txt");

        final DataSet<Tuple2<String, Integer>> wordAndOne = lines.flatMap((String line, Collector<Tuple2<String, Integer>> collector) -> Arrays.stream(line.split(" ")).forEach(word -> collector.collect(Tuple2.of(word, 1)))).returns(Types.TUPLE(Types.STRING, Types.INT));

        final DataSet<Tuple2<String, Integer>> sum = wordAndOne.groupBy(0).sum(1);

        sum.print();
    }
}
