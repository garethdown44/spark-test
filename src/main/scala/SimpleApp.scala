/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// {F5607606-D500-455B-9CCC-01CB6FFB4567},572000,2015-07-10 00:00,"LA8 9LP","D","N","F","FELL CLOSE HOUSE","","KENDAL ROAD","STAVELEY","KENDAL","SOUTH LAKELAND","CUMBRIA","A","A"
// "AB101AA",10,394251,806376,"S92000003","","S08000020","","S12000033","S13002483"
// 

object Mappers {
    def toYearsPricesAndPostcodes(s: String) : ( (Int, String), Double) = {

        val splitted = s.split(",")
        val price = splitted(1).replace("\"", "").toDouble
        val year = splitted(2).substring(0, 4).toInt
        val postcodePrefix = splitted(3).replace("\"", "").split(" ")(0)

        return ( (year, postcodePrefix), price)
    }

    // def toAveragesPerPostcodePerYear(tuple : ( (Int, String), Long) ) = {

    //     return 
    // }
}

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

    val housePrices = sc.textFile("./example2.txt").cache

    val prices = housePrices.map(Mappers.toYearsPricesAndPostcodes)
    val count = prices.count

    val totalPricesTotalCounts = prices.mapValues(x => (x, 1)).reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2) )

    val averagePrices = totalPricesTotalCounts.mapValues(x => x._1 / x._2)

    val mapped = averagePrices.map{ case ((y,p), pr) => (p, (y,pr)) } // ((1995, CB11), 100000) --> (CB11, (1995, 100000))

    println("mapped has these items")
    mapped.foreach(println)

    val firsts = mapped.map{ case (p, (y,pr)) => (p, y % 1995) -> (y, pr)}
    val seconds = mapped.map{ case (p, (y,pr)) => (p, y % 1996) -> (y, pr)}

    println("firsts")
    firsts.foreach(println)

    println("seconds")
    seconds.foreach(println)

    println("joined")
    val joined = firsts.join(seconds)

    joined.foreach(println)

    val percentIncrease = joined.map{ case ( (p,i), ((y1,pr1), (y2,pr2))) => p -> (y2, (pr1 / pr2)*100) }

    println("percent increase:")
    percentIncrease.foreach(println)

    // a -> 1, 100
    // a -> 2, 200
    // a -> 3, 250

    // b -> 1, 300
    // b -> 2, 350

    // c -> 1, 400

    // mod the year by either the start year or the next year to get the same indeces

    // map ->
    //
    // (a,0) -> 1,100
    // and 
    // (a,0) -> 2,200

    // then join them and map back out

    // (a,2) -> (100,200)
    // (a,3) -> (200,250)

    // map values again to get percentage increase


    // within each group we want to zip

    // a -> ( (1, 100), (2, 200) )
    // a -> ( (2, 200), (3, 250) )


    sc.stop
  }
}