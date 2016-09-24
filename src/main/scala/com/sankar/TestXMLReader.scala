package com.sankar

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

object TestXMLReader extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Java Spark SQL basic example")
    .config("spark.sql.warehouse.dir", "file:///TestData/")
    .getOrCreate();
  val df = spark.read
    .format("com.databricks.spark.xml")
    //.option("rootTag", "book")
    .option("rowTag", "book")
    .load("file:///TestData/test.xml")

  //df.show()
  //df.foreach { row => println(row(1)) }

  //df.select("author", "_id").foreach { row => println(row) }
  df.select("_id","title","authors").foreach { row => println(row) }

}