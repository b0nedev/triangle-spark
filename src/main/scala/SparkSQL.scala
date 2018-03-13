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
  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(args)

    // Load sample data
    import com.mongodb.spark._

    // Create SparkSession
    val sparkSession = SparkSession.builder().getOrCreate()

    // Explicitly declaring a schema
        MongoSpark.load[SparkSQL.Character](sparkSession).printSchema()

    //    // Spark SQL
    val characters = MongoSpark.load[SparkSQL.Character](sparkSession)
    characters.createOrReplaceTempView("characters")

    //auction notebook category ranking
    val aucsql = sparkSession.sql("SELECT * FROM characters WHERE site = 'auction'")
    //aucsql.show()

    val auccates = aucsql.select("category").distinct.collect.map(_.getString(0))
    println(auccates.mkString(" "))

    import com.mongodb.spark.MongoSpark

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

  //scalastyle:on method.length

  case class Character(title: String, url: String, site: String, category: String, count: Int, img: String)
}
