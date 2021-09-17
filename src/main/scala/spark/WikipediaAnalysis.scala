package spark

import com.databricks.spark.xml.XmlDataFrameReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

import scala.collection.mutable

object WikipediaAnalysis {
  def main(args: Array[String]): Unit = {
//    findMostUsedWords("src/main/resources/enwiki-20210901-pages-meta-history1.xml-p1p857").foreach(println)
//    findMostChangedArticles("src/main/resources/enwiki-20210901-pages-meta-history1.xml-p1p857").foreach(println)
//    findLargestArticles("src/main/resources/enwiki-20210901-pages-meta-history1.xml-p1p857").foreach(println)
  }

  def findMostUsedWords(filename: String): Array[(String, Long)] = {
    val spark = SparkSession.builder().appName("WikipediaAnalysis").master("local[*]").getOrCreate()
    val df = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "revision")
      .option("excludeAttribute", "true")
      .xml(filename)

    import spark.implicits._
    val texts = df.select("text").as[String]
    texts.flatMap(text => text.split(" ").map(word => word.trim))
      .groupByKey(identity).count().sort(desc("count(1)")).take(10)
  }

  def findMostChangedArticles(filename: String): Array[(String, Int)] = {
    val spark = SparkSession.builder().appName("WikipediaAnalysis").master("local[*]").getOrCreate()
    val df = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      .option("excludeAttribute", "true")
      .xml(filename)

    import spark.implicits._
    df.map(row => (row.getAs[String]("title"), row.getAs[mutable.WrappedArray[Any]]("revision").length))
      .sort(desc("_2")).take(10)
  }

  def findLargestArticles(filename: String): Array[(String, Int)] = {
    val spark = SparkSession.builder().appName("WikipediaAnalysis").master("local[*]").getOrCreate()
    val df = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      .option("excludeAttribute", "true")
      .xml(filename)

    import spark.implicits._
    df.select("title", "revision.text")
      .map(row => (row.getAs[String]("title"), row.getAs[mutable.WrappedArray[String]]("text")
        .reduce((a, b) => if (a.length > b.length) a else b).length)).sort(desc("_2")).take(10)
  }
}
