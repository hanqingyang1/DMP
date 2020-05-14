package cn.itcast.utils

import java.util.Date

import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.time.FastDateFormat
import org.apache.kudu.Schema
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

class KuduHelper {

  private var kuduContext: KuduContext = _

  private val config = ConfigFactory.load("kudu")

  private var spark: SparkSession = _

  private var dataset : Dataset[_] = _

  def this(spark:SparkSession) = {
    this()
    this.spark = spark
    val master = config.getString("kudu.master")
    kuduContext = new KuduContext(master,spark.sparkContext,Some(900000))
  }


  def this(dataset:Dataset[_])={
    this(dataset.sparkSession)
    this.dataset = dataset
  }


  def createKuduTable(tableName:String,schema:Schema,keys:List[String]): Unit ={
    if(kuduContext.tableExists(tableName)){
      kuduContext.deleteTable(tableName)
    }

    import scala.collection.JavaConverters._
    val options = new CreateTableOptions()
      .setNumReplicas(config.getInt("kudu.table.factor"))
      .addHashPartitions(keys.asJava,2)
    kuduContext.createTable(tableName,schema,options)
  }

  def readKuduTable(tableName:String): Option[DataFrame] ={

    import org.apache.kudu.spark.kudu._

    if(kuduContext.tableExists(tableName)){
      val result = spark.read
        .option("kudu.master", kuduContext.kuduMaster)
        .option("kudu.table", tableName)
        .kudu

      Some(result)
    } else {
      None
    }

  }

  def saveToKudu(tableName: String): Unit ={

    if(dataset == null){
      throw new RuntimeException("请从 dataset上开始调用")
    }
    import org.apache.kudu.spark.kudu._
    dataset.write
      .option("kudu.master",kuduContext.kuduMaster)
      .option("kudu.table",tableName)
      .mode(SaveMode.Append)
      .kudu
  }


}

object KuduHelper{

  //将SparkSession转为KuduHelper
  implicit def sparkSessionToKuduHelper(spark:SparkSession):KuduHelper ={
    new KuduHelper(spark)
  }

  //将dataFrame 转为 Kuduelper
  implicit def dataFrameToKuduHelper(dataset:Dataset[_]):KuduHelper={
    new KuduHelper(dataset)
  }

  def formatedDate: String ={
    FastDateFormat.getInstance("yyyyMMdd").format(new Date())
  }
}
