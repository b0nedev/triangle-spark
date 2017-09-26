/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tour

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.bson.Document
import org.bson.types.ObjectId
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.types.StructType


/**
  * The spark SQL code example see docs/1-sparkSQL.md
  */
object SparkSQL extends TourHelper {

  //scalastyle:off method.length
  /**
    * Run this main method to see the output of this quick example or copy the code into the spark shell
    *
    * @param args takes an optional single argument for the connection string
    * @throws Throwable if an operation fails
    */
  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(args)

    // Load sample data
    import com.mongodb.spark._

    //    // Create SparkSession
    val sparkSession = SparkSession.builder().getOrCreate()
    //
    //    // Import the SQL helper
   // val df = MongoSpark.load(sparkSession)
    //    df.printSchema()
    //
    //val df2 = df.orderBy(org.apache.spark.sql.functions.col("count").desc)
    //df2.show()
    //println(df2.count())
    //val df3 = df2.limit(20)
    //println(df3.count())
//    MongoSpark.save(df3.write.option("collection", "products2"))

//    val test = df.groupBy("category")
//    println(test)
    //print(aucsql.head().getString(3))
    //
    //
    //    // Explicitly declaring a schema
        MongoSpark.load[SparkSQL.Character](sparkSession).printSchema()
    //
    //    // Spark SQL
    val characters = MongoSpark.load[SparkSQL.Character](sparkSession)
    characters.createOrReplaceTempView("characters")

    //auction notebook category ranking
    val aucsql = sparkSession.sql("SELECT * FROM characters WHERE site = 'auction'")
    //aucsql.show()

    val auccates = aucsql.select("category").distinct.collect.map(_.getString(0))
    println(auccates.mkString(" "))

    import com.mongodb.spark.MongoSpark

//    for(i <-0 to auccates.length-1){
//      println(auccates(i))
//      val aucdf = sparkSession.sql("SELECT * FROM characters WHERE site = 'auction' and category='" + auccates(i) + "'")
//      aucdf.show()
//      val rankaucdf = aucdf.orderBy(org.apache.spark.sql.functions.col("count").desc).limit(20)
//      rankaucdf.show()
//      //MongoSpark.save(rankaucdf.write.option("collection", "rank").option("mode", "append"))
//      //MongoSpark.write(rankaucdf).option("collection", "rank").option("mode", "append").save()
//    }



//    val testdf = sparkSession.sql("SELECT title, first(url) alias url, first(site) alias site, first(category) alias category, first(count) alias count FROM characters GROUP BY title")
//    println("========test============")
//    testdf.show()
//    //val ranktestdf = testdf.orderBy(org.apache.spark.sql.functions.col("first(count)").desc).limit(20)
//    println("========test2============")
//    testdf.write.format("com.mongodb.spark.sql.DefaultSource").option("collection", "ranks").mode("append").save()



    val aucdf = sparkSession.sql("SELECT title, first(url) as url, first(site) as site, first(category) as category, first(count) as count, first(img) as img FROM characters WHERE site = 'auction' GROUP BY title")
    aucdf.show()
    val rankaucdf = aucdf.orderBy(org.apache.spark.sql.functions.col("count").desc).limit(20)
    rankaucdf.show()
    rankaucdf.write.format("com.mongodb.spark.sql.DefaultSource").option("collection", "ranks").mode("append").save()


    //timon notebook category ranking
    val tmsql = sparkSession.sql("SELECT * FROM characters WHERE site = 'ticketmonster'")
    tmsql.show()

    val tmcates = tmsql.select("category").distinct.collect.map(_.getString(0))
    println(tmcates.mkString(" "))
    println(tmcates(0))

    val tmdf = sparkSession.sql("SELECT title, first(url) as url, first(site) as site, first(category) as category, first(count) as count, first(img) as img  FROM characters WHERE site = 'ticketmonster' GROUP BY title")
    tmdf.show()
    val ranktmdf = tmdf.orderBy(org.apache.spark.sql.functions.col("count").desc).limit(20)
    ranktmdf.show()
    ranktmdf.write.format("com.mongodb.spark.sql.DefaultSource").option("collection", "ranks").mode("append").save()


    //g9 notebook category ranking
    val g9sql = sparkSession.sql("SELECT * FROM characters WHERE site = 'g9'")
    g9sql.show()

    val g9cates = g9sql.select("category").distinct.collect.map(_.getString(0))
    println(g9cates.mkString(" "))
    println(g9cates(0))

    val g9df = sparkSession.sql("SELECT title, first(url) as url, first(site) as site, first(category) as category, first(count) as count, first(img) as img  FROM characters WHERE site = 'g9' GROUP BY title")
    g9df.show()
    val rankg9df = g9df.orderBy(org.apache.spark.sql.functions.col("count").desc).limit(20)
    rankg9df.show()
    rankg9df.write.format("com.mongodb.spark.sql.DefaultSource").option("collection", "ranks").mode("append").save()







//    val tmsql = sparkSession.sql("SELECT * FROM characters WHERE site = 'ticketmonster'")
//    tmsql.show()
//
//    val tmcates = tmsql.select("category").distinct.collect.map(_.getString(0))
//    println(tmcates.mkString(" "))
//    for(i <- 0 to tmcates.length){
//      val tmdf = sparkSession.sql("SELECT * FROM characters WHERE site = 'ticketmonster' and category='" + tmcates(i) + "'")
//      tmdf.show()
//      val ranktmdf = tmdf.orderBy(org.apache.spark.sql.functions.col("count").desc).limit(20)
//      //MongoSpark.save(ranktmdf.write.option("collection", "rank").option("mode", "append"))
//      MongoSpark.write(ranktmdf).option("collection", "rank2").option("mode", "append").save()
//    }


  }

  //scalastyle:on method.length

  case class Character(title: String, url: String, site: String, category: String, count: Int, img: String)
}
