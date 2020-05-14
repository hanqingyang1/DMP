package cn.itcast.report

import org.apache.kudu.Schema
import org.apache.spark.sql.DataFrame

trait ReportProcessor {

  /**
    * 提供源表表明
    * @return
    */
  def sourceTableName():String

  /**
    * 对外提供数据处理
    * @param dataFrame
    * @return
    */
  def process(dataFrame: DataFrame):DataFrame


  /**
    * 提供目标表名
    * @return
    */
  def tatgetTableName():String


  /**
    * 提供目标表Schema信息
    * @return
    */
  def targetTableSchema():Schema

  /**
    * 提供目标表Keys
    * @return
    */
  def targetTableKeys():List[String]
}
