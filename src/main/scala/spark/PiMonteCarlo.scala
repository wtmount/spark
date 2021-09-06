package spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object PiMonteCarlo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PiMonteCarlo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val n = 1000000
    val res = sc.parallelize(0 until n).map(f).reduce(_ + _)
    println("Approximate PI number is " + 4 * res.toDouble / n)
    sc.stop()
  }

  def f(i: Int): Int = {
    val x = Random.nextDouble
    val y = Random.nextDouble
    if ((x * x + y * y) < 1) 1 else 0
  }
}
