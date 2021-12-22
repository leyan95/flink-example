package org.example.day02

import org.apache.flink.streaming.api.scala._

/**
 * Created by leyan95 2021/12/22
 */
object MaxScalaDemo {

  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines = environment.socketTextStream("localhost", 8888)
    val wordAndNum = lines.map(line => {
      val strings = line.split(",")
      (strings(0), strings(1).toInt)
    })
    val keyed = wordAndNum.keyBy(_._1)
    val max = keyed.max(1)
    max.print()
    environment.execute("MaxScalaDemo")
  }
}
