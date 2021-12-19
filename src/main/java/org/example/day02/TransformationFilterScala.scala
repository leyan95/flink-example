package org.example.day02

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Created by leyan95 2021/12/19
 */
object TransformationFilterScala {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numberStream: DataStream[Int] = environment.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val oddStream: DataStream[Int] = numberStream.filter(number => number % 2 != 0)
    oddStream.print()
    environment.execute("TransformationFilterScala")
  }
}
