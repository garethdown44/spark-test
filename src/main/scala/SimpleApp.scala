/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// {F5607606-D500-455B-9CCC-01CB6FFB4567},572000,2015-07-10 00:00,"LA8 9LP","D","N","F","FELL CLOSE HOUSE","","KENDAL ROAD","STAVELEY","KENDAL","SOUTH LAKELAND","CUMBRIA","A","A"
// "AB101AA",10,394251,806376,"S92000003","","S08000020","","S12000033","S13002483"
// 

object Mappers {
    def toYearsPricesAndPostcodes(s: String) : ( (Int, String), Int) = {

        val splitted = s.split(",")
        val price = splitted(1).replace("\"", "").toInt
        val year = splitted(2).substring(0, 4).toInt
        val postcodePrefix = splitted(3).replace("\"", "").split(" ")(0)

        return ( (year, postcodePrefix), price)
    }

    // def toAveragesPerPostcodePerYear(tuple : ( (Int, String), Long) ) = {

    //     return 
    // }
}

object SimpleApp {
  def main(args: Array[String]) {
    /*val logFile = "file:///d:/dev/spark-1.5.1/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))*/

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    //val postcodes = sc.textFile("file:///d:/codepo_gb/Data/CSV/*.csv")
    //val housePrices = sc.textFile("file:///d:/pp-2015.txt").cache

    val housePrices = sc.textFile("file:///c:/dev/spark/example.txt").cache

    val prices = housePrices.map(Mappers.toYearsPricesAndPostcodes)
    val count = prices.count

    val totalPricesTotalCounts = prices.mapValues(x => (x, 1)).reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2) )

    val averagePrices = totalPricesTotalCounts.mapValues(x => x._1 / x._2)

    //val percentIncreasePerYear = averagePrices.reduce( (a, b) => (a._2 + b._2 )

    // ((1995, cb11), 100000)
    // ((1996, cb11), 200000)

    // map to cb11, (1995, 100000)
    //        cb11, (1996, 200000)
    //
    // sort by value._1
    //
    // reduce to pc, (y, percent)
    //
    // map back to (year, postcode), value

    //val percentIncreasePerYear = averagePrices.reduceByKey( (a, b) => a / b)

    //percentIncreasePerYear.foreach(println)

    val mapped = averagePrices.map{ case ((y,p), pr) => (p, (y,pr))} // ((1995, CB11), 100000) --> (CB11, (1995, 100000))

    val drop1ed = mapped.filter { case (k, v) => k > 1 }

    val zipped = mapped.zip(drop1ed)

    zipped.foreach(println)

    sc.stop

    //val averagesPerYear = totalsPerYear.

    //averagePerYear.foreach(println)

    //println("** Average price - %s".format(averagePrice))

    // xs = 1,2,3,4
    // xs
    // ys = xs.drop 1

    // xs.zip(ys) = (1, 2), (2, 3), (3, 4)


    // 1995, 1
    // 1996, 2
    // 1997, 4

    // 1995, (0, 1)
    // 1996, (1, 2)
    // 1997, (2, 4)

    // 1995, 0
    // 1996, 
  }
}