import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SimpleApp {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    //val postcodes = sc.textFile("file:///d:/codepo_gb/Data/CSV/*.csv")
    //val housePrices = sc.textFile("file:///d:/pp-2015.txt").cache

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val housePrices = sc.textFile("file:///d:/pp-complete.csv").cache

    val prices = housePrices.map(toYearsPricesAndPostcodes)

    val filtered = prices.filter{ case ( (y, pc), pr) => pc.startsWith("CB11 4S") }

    val mapped = filtered.map{ case ( (y, pc), pr) => Array(pc, y, pr).mkString(",") }

    mapped.coalesce(1, true).saveAsTextFile("output")

    //val count = prices.count

    // val totalPricesTotalCounts = prices.mapValues(x => (x, 1)).reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2) )

    // val averagePrices = totalPricesTotalCounts.mapValues(x => x._1 / x._2)

    // val mapped = averagePrices.map{ case ((y,p), pr) => (p, (y,pr)) } // ((1995, CB11), 100000) --> (CB11, (1995, 100000))

    // println("mapped has these items")

    // //mapped.coalesce(1,true).saveAsTextFile("./mapped.txt")

    // val firsts = mapped.map{ case (p, (y,pr)) => (p, y % 1995) -> (y, pr)}
    // val seconds = mapped.map{ case (p, (y,pr)) => (p, y % 1996) -> (y, pr)}

    // val joined = firsts.join(seconds)

    // //joined.coalesce(1,true).saveAsTextFile("./joined.txt")

    // val percentIncrease = joined.map{ case ( (p,i), ((y1,pr1), (y2,pr2))) => p -> (y2, pr1, pr2, calcPercent(pr1, pr2) ) }

    // // val filtered = percentIncrease.filter{ case (p, (y2, pr1, pr2, percent)) => p == "CB11" && y2 == 2014 }

    // println("percent increase:")

    // //val finalValues = percentIncrease.coalesce(1,true)

    // //finalValues.saveAsTextFile("./output.txt")

    // val csvValues = filtered.map{ case (p, (y2, pr1, pr2, percent)) => Array(p, y2, pr1, pr2, percent).mkString(",") }

    // csvValues.coalesce(1, true).saveAsTextFile("output")

    sc.stop
  }

  def toYearsPricesAndPostcodes(s: String) : ( (Int, String), Double) = {

        val splitted = s.split(",")
        val price = splitted(1).replace("\"", "").toDouble
        val year = splitted(2).replace("\"", "").substring(0, 4).toInt

        val postcode = splitted(3).replace("\"", "")
        //val postcodePrefix = splitPostcode(0) + splitPostcode(1).substring(1)

        //val postcodePrefixPlusOne = 

        return ( (year, postcode), price)
    }

    def calcPercent(a: Double, b: Double) : Double = {
        return ((b - a) / a) * 100
    }
}