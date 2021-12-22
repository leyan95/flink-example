package org.example.day02

import org.apache.flink.streaming.api.scala._

/**
 * @author 吴建川
 * @date 2021/12/22
 * @version 0.0.1
 */
object TransformationKeysByScala {

  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val words = environment.socketTextStream("localhost", 8888)
    val storages: DataStream[Storage] = words.map(word => {
      val strings: Array[String] = word.split(",")
      Storage.of(strings(0), strings(1), strings(1).toLong)
    })
    val keyed = storages.keyBy(storage => storage.`type`).keyBy(storage => storage.name)
    val sum = keyed.sum("count")
    sum.print()
    environment.execute("TransformationKeysByScala")
  }

  /**
   * Office,book,10
   * Office,book,12
   * Office,pen,60
   * Gift,SouvenirCoin,100
   * Gift,Album,12
   */
  object Storage {
    def of(`type`: String, name: String, count: Long): Storage = new Storage(`type`, name, count)
  }

  final class Storage() {
    var `type`: String = ""
    var name: String = ""
    var count = 0L

    def this(`type`: String, name: String, count: Long) {
      this()
      this.`type` = `type`
      this.name = name
      this.count = count
    }

    override def toString: String = "Storage{" + "type='" + `type` + '\'' + ", name='" + name + '\'' + ", count=" + count + '}'
  }
}
