package cn.edu.sjtu.omnilab.kalin.hz

import cn.edu.sjtu.omnilab.kalin.stlab.{MPoint, CleanseMob}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext.rddToPairRDDFunctions

/**
 * Cleanse and export Hangzhou mobile data into long format.
 * 0,1345057200.000,37809
 * 0,1345083339.812,37809
 * 0,1345143600.000,37809
 * 0,1345179491.410,37809
 * 0,1345193826.864,37809
 * 0,1345194937.249,37809
 * 0,1345194976.465,22694
 * 0,1345194990.815,2136
 */
object TidyMovementJob {
  
  def main(args: Array[String]) {

    if (args.length != 2){
      println("Usage: TidyMovementJob <LOGSET> <LFDATA>")
      sys.exit(0)
    }

    val input = args(0)
    val output = args(1)

    val conf = new SparkConf()
    conf.setAppName("Tidy HZ movement data")
    val spark = new SparkContext(conf)

    val movement = spark.textFile(input)
      .map { line => {
        val tuple = line.split("\t")
        val imsi = imsiPatch(tuple(DataSchema.IMSI))
        val ttime = tuple(DataSchema.TTime).toDouble
        val bs = "%s,%s,%s".format(tuple(DataSchema.BS),tuple(DataSchema.LON),tuple(DataSchema.LAT))
        MPoint(uid=imsi, time=ttime, location=bs)
      }}.filter(x => x.uid != null && x.uid.size > 0)

    val cleaned = CleanseMob.cleanse(movement, minDays=1, minDailyObs=6, minDailyInt=3, tzOffset=8, addNight=true)

    val anonyUsers = cleaned.groupBy(_.uid).zipWithUniqueId
      .flatMap { case ((uid, logs), ucode) => logs.map(log => MPoint(ucode.toString, log.time, log.location))}

    val anonyLocs = anonyUsers.groupBy(_.location).zipWithUniqueId

    // save base station map
    anonyLocs.flatMap { case ((loc, logs), lcode) =>
      logs.map(log => "%s,%s".format(lcode, log.location))}
      .distinct.saveAsTextFile(output + ".bm")

    // save cleaned user movement
    anonyLocs.flatMap { case ((loc, logs), lcode) =>
      logs.map(log => "%s,%.03f,%s".format(log.uid, log.time, lcode))}
      .sortBy(m => m).saveAsTextFile(output)

    spark.stop()
  }

  /**
   * Cleanse IMSI with tail junk data
   */
  def imsiPatch(imsi: String): String = {
    if ( imsi.contains(','))
      return imsi.split(',')(0)
    return imsi
  }
}