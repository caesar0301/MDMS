package cn.edu.sjtu.omnilab.kalin.hz

import java.util.Random

import org.apache.spark._
import org.joda.time.DateTime


/**
 * Sample users with specific ratio.
 */
object SampleUserJob {


  def main(args: Array[String]) {

    if (args.length != 3){
      println("Usage: SampleUserJob <RATIO> <MOVDATA> <SAMPLE>")
      sys.exit(0)
    }

    val conf = new SparkConf()
      .setAppName("SampleUser")
    val sc = new SparkContext(conf)

    val samplingRatio = args(0).toDouble
    val input = args(1)
    val output = args(2)
    val logs = sc.textFile(input)

    val rand = new Random(System.currentTimeMillis());

    logs.map(_.split(",")).groupBy(_(0))
      .filter { case (uid, movdata) => {
        rand.nextDouble() <= samplingRatio
    }}.flatMap { case (uid, movdata) => movdata }
      .map(line => line.mkString(","))
      .saveAsTextFile(output)

    sc.stop()
  }


  /**
   * Parse date value as file name from recording time.
   * @param milliSecond
   * @return
   */
  def parseDay(milliSecond: Long): String = {
    val datetime = new DateTime(milliSecond)
    return "SET%02d%02d".format(datetime.getMonthOfYear, datetime.getDayOfMonth)
  }
}
