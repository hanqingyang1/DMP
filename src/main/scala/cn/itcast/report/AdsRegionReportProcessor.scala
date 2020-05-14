package cn.itcast.report

import cn.itcast.etl.ETLRunner
import cn.itcast.utils.KuduHelper
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import org.apache.spark.sql.DataFrame

object AdsRegionReportProcessor extends ReportProcessor{
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

  val sourceTableName = "ods_table"
  val MID_TEMP_TABLE: String = "kpi_table"

  val kpiSQL =
    s"""
      |SELECT
      |region,city,
      |sum(
      |   case
      |     when(t.requestmode = 1 and processnode >=1) then 1
      |     else 0
      |end
      |)as orginal_req_cnt,
      |sum(
      |   case
      |     when(t.requestmode = 1 and processnode >= 2) then 1
      |     else 0
      |end
      |)as valid_req_cnt,
      |sum(CASE
      |        WHEN (t.requestmode = 1
      |              AND t.processnode = 3) THEN 1
      |        ELSE 0
      |    END) AS ad_req_cnt,
      |sum(CASE
      |        WHEN (t.adplatformproviderid >= 100000
      |              AND t.iseffective = 1
      |              AND t.isbilling =1
      |              AND t.isbid = 1
      |              AND t.adorderid != 0) THEN 1
      |        ELSE 0
      |    END) AS join_rtx_cnt,
      |sum(CASE
      |        WHEN (t.adplatformproviderid >= 100000
      |              AND t.iseffective = 1
      |              AND t.isbilling =1
      |              AND t.iswin = 1) THEN 1
      |        ELSE 0
      |    END) AS success_rtx_cnt,
      |sum(CASE
      |        WHEN (t.requestmode = 2
      |              AND t.iseffective = 1) THEN 1
      |        ELSE 0
      |    END) AS ad_show_cnt,
      |sum(CASE
      |        WHEN (t.requestmode = 3
      |              AND t.iseffective = 1) THEN 1
      |        ELSE 0
      |    END) AS ad_click_cnt,
      |sum(CASE
      |        WHEN (t.requestmode = 2
      |              AND t.iseffective = 1
      |              AND t.isbilling = 1) THEN 1
      |        ELSE 0
      |    END) AS media_show_cnt,
      |sum(CASE
      |        WHEN (t.requestmode = 3
      |              AND t.iseffective = 1
      |              AND t.isbilling = 1) THEN 1
      |        ELSE 0
      |    END) AS media_click_cnt,
      |sum(CASE
      |        WHEN (t.adplatformproviderid >= 100000
      |              AND t.iseffective = 1
      |              AND t.isbilling = 1
      |              AND t.iswin = 1
      |              AND t.adorderid > 20000
      |              AND t.adcreativeid > 200000) THEN floor(t.winprice / 1000)
      |        ELSE 0
      |    END) AS dsp_pay_money,
      |sum(CASE
      |        WHEN (t.adplatformproviderid >= 100000
      |              AND t.iseffective = 1
      |              AND t.isbilling = 1
      |              AND t.iswin = 1
      |              AND t.adorderid > 20000
      |              AND t.adcreativeid > 200000) THEN floor(t.adpayment / 1000)
      |        ELSE 0
      |END) AS dsp_cost_money
      |
      |FROM $sourceTableName t
      |
      |GROUP BY
      |region ,city
      |
    """.stripMargin

  lazy val sqlWithRate =
    s"""
       |SELECT t.*,
       |       t.success_rtx_cnt / t.join_rtx_cnt AS success_rtx_rate,
       |       t.media_click_cnt / t.ad_click_cnt AS ad_click_rate
       |FROM $MID_TEMP_TABLE t
       |WHERE t.success_rtx_cnt != 0
       |  AND t.join_rtx_cnt != 0
       |  AND t.media_click_cnt != 0
       |  AND t.ad_click_cnt != 0
    """.stripMargin

  dataFrame.createOrReplaceTempView(sourceTableName)
  val kpiTable = dataFrame.sparkSession.sql(kpiSQL)
  kpiTable.createOrReplaceTempView(MID_TEMP_TABLE)
  val rateTable = dataFrame.sparkSession.sql(sqlWithRate)
  rateTable

}

  /**
    * 提供目标表名
    *
    * @return
    */
  override def tatgetTableName(): String = {
    "report_ads_region_"+KuduHelper.formatedDate
  }

  /**
    * 提供目标表Schema信息
    *
    * @return
    */
  override def targetTableSchema(): Schema = {

    import scala.collection.JavaConverters._

    new Schema(
      List(
        new ColumnSchemaBuilder("region", Type.STRING).nullable(false).key(true).build(),
        new ColumnSchemaBuilder("city", Type.STRING).nullable(false).key(true).build(),
        new ColumnSchemaBuilder("orginal_req_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("valid_req_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("ad_req_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("join_rtx_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("success_rtx_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("ad_show_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("ad_click_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("media_show_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("media_click_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("dsp_pay_money", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("dsp_cost_money", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("success_rtx_rate", Type.DOUBLE).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("ad_click_rate", Type.DOUBLE).nullable(true).key(false).build()
      ).asJava
    )
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
