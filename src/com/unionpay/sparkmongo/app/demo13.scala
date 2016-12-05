package com.unionpay.sparkmongo.app

import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

/**
  * Created by yhqairqq@163.com on 16/10/18.
  */
object demo13 {
  val db = "crawl_hz"
  val collection = "MainShop_DataOrigin_Unionpay"
  val collection_coupon = "BankCouponInfo"
  val collection_coupon_test = "BankCouponInfoTest"
  val collection_test = "MainShop2"
  val collection_out = "MainShop_DataOrigin_Unionpay_statistics2"
//  val uri = "mongodb://127.0.0.1:33332/"
        val uri = "mongodb://10.15.159.169:30000/"
  val connAliveTime = "15000000000"


  def save2mongo(uri: String, db: String, collection: String, df: DataFrame) {
    val conf = df.sqlContext.sparkContext.getConf
      .set("spark.mongodb.keep_alive_ms", connAliveTime)
      .set("spark.mongodb.output.uri", uri)
      .set("spark.mongodb.output.database", db)
      .set("spark.mongodb.output.collection", collection)

    val writeConfig = WriteConfig(conf)

    /**
      * 其它集中模式
      * SaveMode.ErrorIfExists (default)	"error" (default)
      * SaveMode.Append	"append"
      * SaveMode.Overwrite	"overwrite"
      * SaveMode.Ignore	"ignore"
      */

    df.write.mode(SaveMode.Overwrite).mongo(writeConfig)
  }

  def update2mongo(uri: String, db: String, collection: String, df: DataFrame) {
    val conf = df.sqlContext.sparkContext.getConf
      .set("spark.mongodb.keep_alive_ms", connAliveTime)
      .set("spark.mongodb.output.uri", uri)
      .set("spark.mongodb.output.database", db)
      .set("spark.mongodb.output.collection", collection)

    val writeConfig = WriteConfig(conf)

    /**
      * 其它集中模式
      * SaveMode.ErrorIfExists (default)	"error" (default)
      * SaveMode.Append	"append"
      * SaveMode.Overwrite	"overwrite"
      * SaveMode.Ignore	"ignore"
      */

    df.write.mode(SaveMode.Overwrite).mongo(writeConfig)
  }


  def mongoDF(ss: SparkSession, uri: String, db: String, collection: String): DataFrame = {
    val config = ss.sparkContext.getConf
      .set("spark.mongodb.keep_alive_ms", connAliveTime)
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.input.database", db)
      .set("spark.mongodb.input.collection", collection)
      //todo https://docs.mongodb.com/spark-connector/configuration/
      .set("spark.mongodb.input.readPreference.name", "primaryPreferred")
      .set("spark.mongodb.input.partitioner", "MongoSamplePartitioner")
      .set("spark.mongodb.input.partitionerOptions.partitionSizeMB", "1024")
    val readConfig = ReadConfig.create(config)
    ss.read.mongo(readConfig)

  }

  def findOne(ss: SparkSession, id: String, df: DataFrame) = {


  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
//      .setMaster("local[*]")
      .setAppName("demo13")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()



    val df_mainshop = mongoDF(spark, uri, db, collection)
    //    val df_coupon_test = mongoDF(spark,uri,db,collection_coupon_test)
    val df = mongoDF(spark, uri, db, collection_out)



        val filterRDD = df_mainshop.rdd.map(x=>(x.getAs[String]("brand"),x.getAs[String]("shopName"))).filter(x=>x._1==x._2).map(x=>(x._1,1))
        .reduceByKey((a,b)=>a+b).sortBy(_._2,true,4)
//          .filter(args=>args._1.size<3 || args._2.size<3).sortBy(x=>x._1,true,5)
//          .sortBy()
////        filterRDD.saveAsTextFile("hours3.txt")
//          .rdd.filter(x=>x!="")
    //    println(filterdf.count())

    //
    //    rows.collect().foreach(println)
    //
    //    //    filterDF.show()
    ////    val rdddf = spark.sparkContext.parallelize(Seq(df.collect()))
    //
    ////    val rowRdd = rdddf.map(v => Row(v: _*))
    ////    rowRdd.collect().foreach(println)
    //    val crdd = rows.flatMap(args => {
    //      var str = args(1).toString
    //      str = str.toLowerCase().replaceAll("[.,!?\n]", " ")
    //      str.split(" ")
    //    }).map(word=>(word,1)).reduceByKey((x,y)=>x+y)
    //
    ////  .map(word => (word, 1)).reduceByKey((a, b) => a + b)
    //
    //    crdd.collect.foreach(println)
    //
    //    val aStruct = new StructType(Array(StructField("word",StringType,nullable = true),StructField("count",IntegerType,nullable = true)))
    //    aStruct.printTreeString()
    //
    //    //todo rdd->df
    //    val outdf = spark.createDataFrame(crdd)
    //
    //    val struct2 =  outdf.schema
    //
    //

    val outdf = spark.createDataFrame(filterRDD);

        save2mongo(uri, db, collection_out, outdf)
    spark.stop()


  }

}
