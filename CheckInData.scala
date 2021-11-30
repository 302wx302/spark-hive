package com.checkIn

import java.text.SimpleDateFormat

import com.common.{DateUtil, HbaseUtil, SparkUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer


/**
  * @ProjectName: ChannelAnalysis
  * @Package: com.checkIn
  * @ClassName: CheckInData
  * @Author: Suns
  * @Descriptions: 解析T-2天之前的数据，可接受一个或两个日期参数
  * @CreateTime: 2019/10/22
  */
object CheckInData {

  //conn链接
  private val conn: Configuration = HbaseUtil.getConn()
  //列簇
  private val columnFamily : String = "cf"
  //列名
  private val columns : List[String] = List("checkInNumTotal", "checkInNumPE", "checkInNumSBD", "checkInNumWeb", "airportCode", "createDate")
  //获取当前系统日期
  private val today : String = DateUtil.getNowDate()

  def main(args: Array[String]): Unit = {
      //创建sparkContext、sparkSession链接
      val (ss, sc): (SparkSession, SparkContext) = SparkUtil.getSessionContext("CheckInDataTest")

      //获取传入日期
      val dates : ArrayBuffer[String] = DateUtil.getFlightDate(args)
      val afferentStartDate : String = dates(0)
      val afferentEndDate : String = dates(1)
      val flightStartDate : String = dates(2)
      val flightEndDate : String = dates(3)

      //选择cua_caci库
//      ss.sql("use cua_caci")
      //数据是T-2天及之前3天的数据，做一个临时表temp
     //chkin_pid_nbr_new as chkin_pid_nbr，后改为使用agent：chkin_agt_wrk_nbr统计
      val temp = ss.sql(
          s"""
             |select fst_leg_dpt_dt,dpt_airpt_cd,chkin_agt_wrk_nbr as chkin_pid_nbr,et_tkt_no,airln_cd,webcki_ind
             |from cua_ods.tcn_cpd
             |where afferent_date >= $afferentStartDate and afferent_date <= $afferentEndDate
             |and fst_leg_dpt_dt >= $flightStartDate and fst_leg_dpt_dt <= $flightEndDate
           """.stripMargin
      ).persist(StorageLevel.MEMORY_AND_DISK_SER).createTempView("temp")

//      ss.catalog.cacheTable("temp")

      /*
        * 通过临时表进行查询得到数据
        * 这里et_tkt_no判断非空，只判断其是否为空串
        */
      val checkInChannel = ss.sql(
          s"""
             |select t1.fst_leg_dpt_dt as flightDate,t1.checkInNumTatol as checkInNumTatol,t2.checkInNumPE as checkInNumPE,t3.checkInNumSBD as checkInNumSBD,t4.checkInNumWeb as checkInNumWeb,t1.dpt_airpt_cd as airportCode
             |from(
                 |select fst_leg_dpt_dt,dpt_airpt_cd,cast (count(1) as Int) as checkInNumTatol
                 |from temp where dpt_airpt_cd = 'PKX' and chkin_pid_nbr <> '' and chkin_pid_nbr is not null and et_tkt_no <> '' and et_tkt_no is not null and airln_cd = 'KN'
                 |group by fst_leg_dpt_dt,dpt_airpt_cd
             |) as t1
             |left join(
                 |select fst_leg_dpt_dt,cast (count(1) as Int) as checkInNumPE
                 |from temp where dpt_airpt_cd = 'PKX' and et_tkt_no <> '' and et_tkt_no is not null and airln_cd = 'KN' and chkin_pid_nbr = '70778'
                 |group by fst_leg_dpt_dt
             |) as t2 on t1.fst_leg_dpt_dt = t2.fst_leg_dpt_dt
             |left join(
                 |select fst_leg_dpt_dt,cast (count(1) as Int) as checkInNumSBD
                 |from temp where dpt_airpt_cd = 'PKX' and et_tkt_no <> '' and et_tkt_no is not null and airln_cd = 'KN' and chkin_pid_nbr = '71008'
                 |group by fst_leg_dpt_dt
             |) as t3 on t1.fst_leg_dpt_dt = t3.fst_leg_dpt_dt
             |left join(
                 |select fst_leg_dpt_dt,cast (count(1) as Int) as checkInNumWeb
                 |from temp where dpt_airpt_cd = 'PKX' and et_tkt_no <> '' and et_tkt_no is not null and airln_cd = 'KN' and webcki_ind = '1'
                 |group by fst_leg_dpt_dt
             |)as t4 on t1.fst_leg_dpt_dt = t4.fst_leg_dpt_dt order by flightDate
           """.stripMargin
      )
      //println(checkInChannel)
      //创建put对象,传递rowKey，并插入数据

      checkInChannel.rdd.map(x => {
          val put: Put = new Put(Bytes.toBytes(x.getString(0)))
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns(0)), Bytes.toBytes(if (x.isNullAt(1)) {0}else{x.getInt(1)}))
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns(1)), Bytes.toBytes(if (x.isNullAt(2)) {0}else{x.getInt(2)}))
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns(2)), Bytes.toBytes(if (x.isNullAt(3)) {0}else{x.getInt(3)}))
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns(3)), Bytes.toBytes(if (x.isNullAt(4)) {0}else{x.getInt(4)}))
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns(4)), Bytes.toBytes(x.getString(5)))
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns(5)), Bytes.toBytes(today))
          (new ImmutableBytesWritable, put)
      }).saveAsNewAPIHadoopDataset(conn)

      ss.stop()
  }


}
