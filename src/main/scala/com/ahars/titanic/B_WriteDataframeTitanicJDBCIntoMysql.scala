package com.ahars.titanic

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object B_WriteDataframeTitanicJDBCIntoMysql {

  def main(args: Array[String]) {

    // Configuration de Spark
    val conf = new SparkConf()
      .setAppName("write-dataframe-titanic-into-mysql")
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
