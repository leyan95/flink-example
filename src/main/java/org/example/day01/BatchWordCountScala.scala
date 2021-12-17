package org.example.day01

import org.apache.flink.api.scala._

/**
 * @author 吴建川
 * @version 0.0.1
 * @date 2021/12/15
 */
object BatchWordCountScala {

  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val lines: DataSet[String] = environment.readTextFile("E:\\a.txt")

    val wordAndOne: DataSet[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))

    val sum: DataSet[(String, Int)] = wordAndOne.groupBy(0).sum(1)

    sum.print()
  }
}
