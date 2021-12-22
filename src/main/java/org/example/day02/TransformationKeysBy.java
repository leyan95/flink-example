package org.example.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wujianchuan 2021/12/19
 * @version 1.0
 */
public class TransformationKeysBy {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> words = environment.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Storage> wordAndOne = words.map((MapFunction<String, Storage>) s -> {
            final String[] strings = s.trim().split(",");
            return Storage.of(strings[0], strings[1], Long.valueOf(strings[2]));
        });
        // 分组
        KeyedStream<Storage, String> keyed = wordAndOne.keyBy(wordCount -> wordCount.type).keyBy(storage -> storage.name);
        // 聚合
        SingleOutputStreamOperator<Storage> sum = keyed.sum("count");
        sum.print();
        environment.execute("TransformationKeysBy");
    }

    /**
     * Office,book,10
     * Office,book,12
     * Office,pen,60
     * Gift,SouvenirCoin,100
     * Gift,Album,12
     */
    public final static class Storage {
        public String type;
        public String name;
        public Long count;

        public Storage() {
        }

        public Storage(String type, String name, Long count) {
            this.type = type;
            this.name = name;
            this.count = count;
        }

        public static Storage of(String type, String name, Long count) {
            return new Storage(type, name, count);
        }

        @Override
        public String toString() {
            return "Storage{" +
                    "type='" + type + '\'' +
                    ", name='" + name + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}
