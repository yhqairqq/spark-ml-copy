package com.unionpay.spark.mllib.graphx

import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

/**
  * Created by yhqairqq@163.com on 2016/11/10.
  */
object graphxDemo1 {
  def main(args: Array[String]) {


    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("graphxdemo1")


    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    //create an rdd for the vertices
    val users: RDD[((VertexId), (String, String))] =
      spark.sparkContext.parallelize(Array(
        (3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))
      ))

    //create an rdd for edges
    val relationships:RDD[Edge[String]]=
      spark.sparkContext.parallelize(Array(
        Edge(3L,7L,"collab"),Edge(5L,3L,"advisor"),
        Edge(2L,5L,"colleague"),Edge(5L,7L,"pi")
      ))

    //defile a default user in case there are relationship with missing user
    val defaultUser = ("John Doe","Missing")
    val graph = Graph(users,relationships,defaultUser)

    //count all the users which are postdoc
    graph.vertices.filter{
      case (id,(name,pos))=>pos=="postdoc"
    }.foreach(println)

    //count all the edges where scr > dst
    graph.edges.filter{
      e=>e.srcId<e.dstId
    }.foreach(println)


  }

}
