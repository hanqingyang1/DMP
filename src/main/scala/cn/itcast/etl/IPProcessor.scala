package cn.itcast.etl

import com.maxmind.geoip.{Location, LookupService}
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.{Dataset, Row}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

object IPProcessor extends Processor{



  override def process(dataset:Dataset[Row]): Dataset[Row] = {

    //处理数据
    val converted = dataset.rdd.mapPartitions(ConvertIp)

    //修改schema
    val schema = dataset.schema
      .add("region", StringType)
      .add("city", StringType)
      .add("longitude", DoubleType)
      .add("latitude", DoubleType)

    val datasetWithIp = dataset.sparkSession.createDataFrame(converted,schema)
    datasetWithIp


  }

  def ConvertIp(iterator:Iterator[Row]):Iterator[Row]={
    val dbseacher = new DbSearcher(new DbConfig(),"dataset/ip2region.db")

    val lookUpService = new LookupService(
      "dataset/GeoLiteCity.dat",
      LookupService.GEOIP_MEMORY_CACHE
    )

    iterator.map(row => {
      val ip = row.getAs[String]("ip")
      
      val regionAll = dbseacher.btreeSearch(ip).getRegion
      val region = regionAll.split("\\|")(2)
      val city = regionAll.split("\\|")(3)

      val location: Location = lookUpService.getLocation(ip)
      val longitude = location.longitude.toDouble
      val latitude = location.latitude.toDouble

      val rowSeq = row.toSeq :+ region :+ city :+ longitude :+ latitude
      Row.fromSeq(rowSeq)

    })
  }

}
