package com.package1
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
object json{
      def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel((Level.ERROR))
        val spark: SparkSession = SparkSession.builder
          .appName("testing")
          .master("local[2]")
          .getOrCreate()
        import spark.implicits._
        val rawData = spark.read.option("multiline","true").json("D:\\untitled1\\sample.json")
        // making menu table
        val menu = rawData.withColumnRenamed("id", "key").select("key","type","name","ppu").toDF()

        val batDF = rawData.withColumnRenamed("id", "menuKey")
          .select("menuKey","batters.batter")

        val batters = batDF.select($"menuKey",explode($"batter").alias("batters"))
          .select("menuKey", "batters.*")
          .withColumnRenamed("id","batID")
          .toDF()

        val topDF = rawData.withColumnRenamed("id", "menuKey")
          .select("menuKey","topping")

        val toppings = topDF.select($"menuKey",explode($"topping").alias("toppings"))
          .select("menuKey", "toppings.*")
          .withColumnRenamed("id","topID")
          .toDF()

// sending data to db
        menu.write.format("jdbc")
          .option("url","jdbc:mysql://localhost:3306/" + "target")
          .option("dbtable", "menuTable")
          .option("user", "root")
          .option("password", "Saad786b")
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .mode(SaveMode.Overwrite)
          .save()

        toppings.write.format("jdbc")
          .option("url","jdbc:mysql://localhost:3306/" + "target")
          .option("dbtable", "toppingsTable")
          .option("user", "root")
          .option("password", "Saad786b")
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .mode(SaveMode.Overwrite)
          .save()


        batters.write.format("jdbc")
          .option("url","jdbc:mysql://localhost:3306/" + "target")
          .option("dbtable", "battersTable")
          .option("user", "root")
          .option("password", "Saad786b")
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .mode(SaveMode.Overwrite)
          .save()



      }
    }