package org.example.day1

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

/**
 * Created by leyan95 2021/12/14
 */
object WordCountScala {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val lines: DataStream[String] = environment.socketTextStream("localhost", 8888)

    val words: DataStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne: DataStream[(String, Int)] = words.map((_, 1))

    val keyed: KeyedStream[(String, Int), Tuple] = wordAndOne.keyBy(0)

    val sumed: DataStream[(String, Int)] = keyed.sum(1)

    sumed.print()

    environment.execute("wordCountScala")
  }
}
