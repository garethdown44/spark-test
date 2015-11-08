import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SimpleApp {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    //val postcodes = sc.textFile("file:///d:/codepo_gb/Data/CSV/*.csv")
    //val housePrices = sc.textFile("file:///d:/pp-2015.txt").cache

    val housePrices = sc.textFile("./example3.txt").cache

    val prices = housePrices.map(toYearsPricesAndPostcodes)

    //val mapped = prices.map{ case ( (y, pc), pr) => Array(pc, y, pr).mkString(",") }
    //mapped.coalesce(1, true).saveAsTextFile("output")

    val meanOfX = prices.map(x => x._1._1.toDouble).reduce((x, y) => x + y) / prices.count
    val meanOfY = prices.map(x => x._2).reduce((x, y) => x + y) / prices.count
    val meanOfXYs = prices.map(x => x._1._1 * x._2).reduce((x, y) => x + y) / prices.count
    val meanOfXSquared = prices.map(x => Math.pow(x._1._1, 2)).reduce((x, y) => x + y) / prices.count

    println("mean of x")
    println(meanOfX)

    println("mean of y")
    println(meanOfY)

    println("mean of xys")
    println(f"$meanOfXYs%2.2f")

    println("mean of x squared")
    println(meanOfXSquared)

    val m = (( (meanOfX * meanOfY) - meanOfXYs)) / ((Math.pow(meanOfX, 2) - meanOfXSquared).toDouble)

    println("m")
    println(f"$m%2.2f")

    val b = meanOfY - (m * meanOfX)

    val y1995 = (m * 1995) + b
    println("y 1995")
    println(y1995)

    val calcY = (mp : Double, x : Double, bp : Double) => (mp * x) + bp

    val range = 1995 until 2015

    val ys = range.map(x => calcY(m, x, b))

    ys.foreach(println)

    //val b = prices.reduce( (a, b) => 1 + b._2)


    //println(meanOfX.toString)

    

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