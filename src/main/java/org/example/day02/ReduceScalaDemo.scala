package org.example.day02

import org.apache.flink.streaming.api.scala._

/**
 * Created by leyan95 2021/12/22
 */
object ReduceScalaDemo {

  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines = environment.socketTextStream("localhost", 8888)
    val keyed = lines.flatMap(_.split(" ")).map((_, 1)).keyBy(t => t._1)
    val reduced = keyed.reduce((m, n) => {
      val key = m._1
      val v1 = m._2
      val v2 = n._2
      (key, v1 + v2)
    })
    reduced.print()
    environment.execute("TransformationReduceScala")
  }
}
