package org.example.day02

import org.apache.flink.streaming.api.scala._

/**
 * @author 吴建川
 * @date 2021/12/20
 * @version 0.0.1
 */
object TransformationKeyByScala {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val words: DataStream[String] = environment.socketTextStream("localhost", 8888)
    val wordAndOne: DataStream[WordCountScala] = words.map(word => WordCountScala.of(word, 1L))
    val keyed: KeyedStream[WordCountScala, String] = wordAndOne.keyBy(wordCount => wordCount.word)
    val sum: DataStream[WordCountScala] = keyed.sum("count")
    sum.print()
    environment.execute("TransformationKeyByScala")
  }

  object WordCountScala {
    def of(word: String, count: Long) = new WordCountScala(word, count)
  }

  final class WordCountScala() {
    var word: String = ""
    var count = 0L

    def this(word: String, count: Long) {
      this()
      this.word = word
      this.count = count
    }

    override def toString: String = "WordCount{" + "word='" + word + '\'' + ", count=" + count + '}'
  }
}
