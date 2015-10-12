package com.ahars.babies

import java.util.Properties

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object C_WriteDataframeBabiesJDBCIntoMysql {

  def main(args: Array[String]) {
    // Configuration de la connexion à la database
    val url = "jdbc:mysql://localhost:3306/sandbox"
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")

    // Création d'une structure de données pour le Dataframe
    val schema = StructType(Array(
      StructField("bwt", DataTypes.IntegerType),
      StructField("gestation", DataTypes.IntegerType),
      StructField("parity", DataTypes.IntegerType),
      StructField("age", DataTypes.IntegerType),
      StructField("height", DataTypes.IntegerType),
      StructField("weight", DataTypes.IntegerType),
      StructField("smoke", DataTypes.IntegerType),
      StructField("education", DataTypes.IntegerType)
    ))

    // Configuration de Spark
    val conf = new SparkConf()
      .setAppName("queries-on-dataframe-babies")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    // Traitement du fichier babies23.txt
    val lines = sc.textFile("src/main/resources/babies23.txt")
      .map(line => line.dropWhile(_ == ' '))
      .filter(line => !line.startsWith("id"))
      .map(line => line.replaceAll("\\s+", ";"))
      .map(_.split(";"))

    // Récupération des champs souhaités
    val rows = lines.map(fields => Row(fields(6).toInt, fields(4).toInt, fields(7).toInt, fields(9).toInt,
      fields(11).toInt, fields(12).toInt, fields(20).toInt, fields(10).toInt
    ))

    // Création du Dataframe à partir des champs récupérés
    val df = sqlc.createDataFrame(rows, schema)

    // Sauvegarde du résultat de la requête dans une table TABLE1
    // SELECT smoke, count(*) FROM df GROUP BY smoke;
    df.groupBy("smoke").count()
      .withColumnRenamed("smoke", "a").withColumnRenamed("count", "b")
      .write.format("jdbc").jdbc(url, "TABLE1", prop)


    // Lecture de la table TABLE1 depuis la database
      sqlc.read.format("jdbc").jdbc(url, "TABLE1", prop).show()
  }
}
