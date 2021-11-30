package com.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job

/**
  * @ProjectName: ChannelAnalysis 
  * @Package: com.common
  * @ClassName: HbaseUtil
  * @Author: Suns
  * @Descriptions: 提供链接hbase链接
  * @CreateTime: 2019/10/22 
  */
object HbaseUtil {


  //所需参数
  //cua_test:channel_checkin_info_test 测试数据表
  private val zk_list : String = "kn52.cu-air.com:2181,kn55.cu-air.com:2181,kn56.cu-air.com:2181"
  private val tableName : String = "cua_caci:channel_checkin_info"  //生产数据表
  //private val tableName : String = "cua_test:channel_checkin_info_test"

  def getConn():  Configuration = {

    //创建链接
    val configuration : Configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum", zk_list)
    configuration.set(org.apache.hadoop.hbase.mapred.TableOutputFormat.OUTPUT_TABLE, tableName)

    //Job
    val job : Job = Job.getInstance(configuration)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Put])
    job.setOutputFormatClass(classOf[TableOutputFormat[NullWritable]])

    job.getConfiguration
  }
}
