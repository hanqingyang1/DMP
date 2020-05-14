package cn.itcast.report

import org.apache.spark.sql.SparkSession

object DailyReportRunner {

  def main(args: Array[String]): Unit = {

    import cn.itcast.utils.SparkConfigHelper._
    import cn.itcast.utils.KuduHelper._
    //创建SparkSession
    val spark = SparkSession.builder()
      .appName("")
      .master("local[6]")
      .loadConfig()
      .getOrCreate()

    val processors = List[ReportProcessor](
      NewRegionReportProcessor,
      AdsRegionReportProcessor
    )

    for (processor <- processors){

      val source = spark.readKuduTable(processor.sourceTableName())

      if(source.isDefined){
        val sourceDf = source.get

        val result = processor.process(sourceDf)

        //数据落地
        spark.createKuduTable(processor.tatgetTableName(),processor.targetTableSchema(),processor.targetTableKeys())

        result.saveToKudu(processor.tatgetTableName())
      }
    }
  }

}
