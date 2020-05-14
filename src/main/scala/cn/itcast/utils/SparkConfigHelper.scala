package cn.itcast.utils

import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

class SparkConfigHelper(builder: SparkSession.Builder) {

  val config = ConfigFactory.load("spark")
  def loadConfig(): SparkSession.Builder ={

    import scala.collection.JavaConverters._
     val entrySet = config.entrySet().asScala

    for (entry <- entrySet){
      val key = entry.getKey
      val value = entry.getValue.unwrapped().asInstanceOf[String]

      val origin = entry.getValue.origin().filename()
      if(StringUtils.isNotBlank(origin)){
//        println(key,value)
        builder.config(key,value)
      }
    }
    builder
  }
}

object SparkConfigHelper{

  implicit def sparkToHelper(builder:SparkSession.Builder):SparkConfigHelper={
    new SparkConfigHelper(builder)
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[6]")
      .loadConfig()
      .getOrCreate()
  }
}