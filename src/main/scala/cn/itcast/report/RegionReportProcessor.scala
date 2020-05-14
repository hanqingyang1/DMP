package cn.itcast.report

import cn.itcast.etl.ETLRunner
import cn.itcast.utils.KuduHelper
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import org.apache.spark.sql.SparkSession

object RegionReportProcessor {

  def main(args: Array[String]): Unit = {

    import cn.itcast.utils.SparkConfigHelper._
    import cn.itcast.utils.KuduHelper._
    //创建SparkSession
    val spark = SparkSession.builder()
      .appName("report_data_region")
      .master("local[6]")
      .loadConfig()
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    //读取ODS数据
    val souce = spark.readKuduTable(SOURCE_TABLE_NAME)

    if(souce.isEmpty) return

    val sourceDf = souce.get

    //按照省市进行分组
    val dataFrame = sourceDf.groupBy('region, 'city)
      .agg(count("*") as "count")
      .select('region,'city,'count)

    dataFrame.show()
    //将数据落地到report表中
    spark.createKuduTable(TARGET_TABLE_NAME,schema,keys)
    dataFrame.saveToKudu(TARGET_TABLE_NAME)
  }

  private val SOURCE_TABLE_NAME = ETLRunner.ODS_TABLE_NAME

  private val TARGET_TABLE_NAME = "report_data_region_"+KuduHelper.formatedDate

  private val keys = List("region","city")

  import scala.collection.JavaConverters._
  private val schema = new Schema(
    List(
      new ColumnSchemaBuilder("region",Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("city",Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("count",Type.INT64).nullable(false).key(true).build()

    ).asJava
  )

}
