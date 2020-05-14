package cn.itcast.report

import cn.itcast.etl.ETLRunner
import cn.itcast.utils.KuduHelper
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import org.apache.spark.sql.DataFrame

object NewRegionReportProcessor extends ReportProcessor{
  /**
    * 提供源表表明
    *
    * @return
    */
  override def sourceTableName(): String = {
    ETLRunner.ODS_TABLE_NAME
  }

  /**
    * 对外提供数据处理
    *
    * @param dataFrame
    * @return
    */
override def process(dataFrame: DataFrame): DataFrame = {

  import dataFrame.sparkSession.implicits._
  import org.apache.spark.sql.functions._

  dataFrame.groupBy('region,'city)
    .agg(count("*") as "count")
    .select('region,'city,'count)


}

  /**
    * 提供目标表名
    *
    * @return
    */
  override def tatgetTableName(): String = {
    "report_data_region_"+KuduHelper.formatedDate
  }

  /**
    * 提供目标表Schema信息
    *
    * @return
    */
  override def targetTableSchema(): Schema = {
    import scala.collection.JavaConverters._
    val schema = new Schema(
      List(
        new ColumnSchemaBuilder("region",Type.STRING).nullable(false).key(true).build(),
        new ColumnSchemaBuilder("city",Type.STRING).nullable(false).key(true).build(),
        new ColumnSchemaBuilder("count",Type.INT64).nullable(false).key(true).build()

      ).asJava
    )
    schema
  }

  /**
    * 提供目标表Keys
    *
    * @return
    */
  override def targetTableKeys(): List[String] = {
    List("region","city")
  }
}
