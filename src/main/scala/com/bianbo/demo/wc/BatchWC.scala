package com.bianbo.demo.wc

import org.apache.flink.api.scala._

object BatchWC {
  def main(args: Array[String]): Unit = {
    //创建一个批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val inputDataSet: DataSet[String] = env.readTextFile("/root/IdeaProjects/flinkDemo/src/main/resources/word.txt")
    // 基于 DataSet做转换，首先按空格分词打散，然后按照word作为key做group by
    val resultDataSet: DataSet[(String, Int)] = inputDataSet
      .flatMap(_.split(" "))    // 分词得到所有word构成的数据集
      .map( (_, 1) )    // 转换成一个二元组 (word, count)
      .groupBy(0)    // 以二元组中第一个元素作为 key分组
      .sum(1)    // 聚合二元组中第二个元素的值

    // 打印输出
    resultDataSet.print()
  }

}
