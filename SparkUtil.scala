package com.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ProjectName: ChannelAnalysis
  * @Package: com.common
  * @ClassName: SparkUtil
  * @Author: Suns
  * @Descriptions: 提供sparkContext、sparkSession的链接
  * @CreateTime: 2019/10/22 
  */
object SparkUtil {
  def getSessionContext(name: String): (SparkSession, SparkContext) = {

    val conf = new SparkConf()
      .setAppName(name)
//      .setMaster("local[*]")

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc = ss.sparkContext

    (ss, sc)
  }
}
