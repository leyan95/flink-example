package org.example.day02

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Created by leyan95 2021/12/19
 */
object TransformationFlatMapScala {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val linesStream: DataStream[String] = environment.fromElements("flink spark hadoop", "java scala flink kafka")
    val wordsStream: DataStream[String] = linesStream.flatMap(_.split(" "))
    wordsStream.print()
    environment.execute("TransformationFlatMapScala")
  }
}
