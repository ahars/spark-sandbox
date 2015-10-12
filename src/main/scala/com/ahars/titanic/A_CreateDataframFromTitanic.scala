package com.ahars.titanic

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object A_CreateDataframFromTitanic {

  def main(args: Array[String]) {

    // Configuration de Spark
    val conf = new SparkConf()
      .setAppName("create-dataframe-from-titanic")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    val df = sqlc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/main/resources/titanic.csv")

    df.show()
    df.printSchema()
  }
}
