package com.unionpay.spark.mllib.lsa

import java.nio.charset.Charset

import com.google.common.hash.Hashing

/**
  * Created by yhqairqq@163.com on 2016/11/9.
  */
object lsademo {

  def hashId(str:String)={
    Hashing.md5().hashString(str,Charset.forName("utf-8")).asLong()
  }
  def termDocWeight(termFrequencyInDoc:Int,totalTermsInDoc:Int,termFreqInCorpus:Int,totalDocs:Int):Double={

    val tf = termFrequencyInDoc.toDouble/totalTermsInDoc

    val docFreq = totalDocs.toDouble/termFreqInCorpus

    val idf = math.log(docFreq)
    tf*idf
  }












  def main(args: Array[String]) {


    val x:String ="yanghuanqing"
     println(hashId(x))
  }

}
