/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    /*val logFile = "file:///d:/dev/spark-1.5.1/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))*/

    val fileName = "file:///d:/pp-complete.csv"
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(fileName)
    val prices = textFile.map(_.split(",")(1).replace("\"", "").toDouble).cache
    val count = prices.count
    val averagePrice = prices.sum / count

    println("** Average price - %s".format(averagePrice))
  }
}