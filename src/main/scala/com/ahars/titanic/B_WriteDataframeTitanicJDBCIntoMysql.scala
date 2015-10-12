package com.ahars.titanic

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object B_WriteDataframeTitanicJDBCIntoMysql {

  def main(args: Array[String]) {

    // Configuration de la connexion à la database
    val url = "jdbc:mysql://localhost:3306/sandbox"
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")

    // Configuration de Spark
    val conf = new SparkConf()
      .setAppName("write-dataframe-titanic-into-mysql")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    val df = sqlc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/main/resources/titanic.csv")

    val groupByPclass = df.groupBy("Pclass").count()
    val groupByEmbarked = df.groupBy("Embarked").count()
    val groupBySurvived = df.groupBy("Survived").count()

    // Write into Mysql
    groupByPclass.write.format("jdbc").jdbc(url, "titanicgroupbypclass", prop)
    groupByEmbarked.write.format("jdbc").jdbc(url, "titanicgroupbyembarked", prop)
    groupBySurvived.write.format("jdbc").jdbc(url, "titanicgroupbysurvived", prop)

    // Read from Mysql
    sqlc.read.format("jdbc").jdbc(url, "titanicgroupbypclass", prop).show()
    sqlc.read.format("jdbc").jdbc(url, "titanicgroupbyembarked", prop).show()
    sqlc.read.format("jdbc").jdbc(url, "titanicgroupbysurvived", prop).show()
  }
}
