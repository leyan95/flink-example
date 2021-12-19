package org.example.day02

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 * Created by leyan95 2021/12/19
 */
object TransformationMapScala {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numberStream: DataStream[Int] = environment.fromElements(1, 2, 3, 4, 5)
    val doubleNumberStream: DataStream[Int] = numberStream.map(number => number * 2)
    val stringDoubleNumberStream: DataStream[String] = doubleNumberStream.map(new RichMapFunction[Int, String] {

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        print("open")
      }

      override def map(in: Int): String = "number - " + in

      override def close(): Unit = {
        super.close()
        print("close")
      }
    })
    stringDoubleNumberStream.print()
    environment.execute("TransformationMapScala")
  }
}
