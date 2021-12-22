package org.example.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wujianchuan 2021/12/19
 * @version 1.0
 */
public class ReduceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> words = environment.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<WordCount> wordAndOne = words.map((MapFunction<String, WordCount>) s -> WordCount.of(s, 1L));
        // 分组
        KeyedStream<WordCount, String> keyed = wordAndOne.keyBy(wordCount -> wordCount.word);
        // 聚合
        SingleOutputStreamOperator<WordCount> sum = keyed.reduce((ReduceFunction<WordCount>) (wordCount1, wordCount2) -> {
            wordCount1.count += wordCount2.count;
            return wordCount1;
        });
        sum.print();
        environment.execute("TransformationKeyBy");
    }

    public final static class WordCount {
        public String word;
        public Long count;

        public WordCount() {
        }

        private WordCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }

        public static WordCount of(String word, Long count) {
            return new WordCount(word, count);
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}
