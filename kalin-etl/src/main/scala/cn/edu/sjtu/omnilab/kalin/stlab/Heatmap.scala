package cn.edu.sjtu.omnilab.kalin.stlab

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


/**
 * @param interval The time range to generate an observation
 * @param loc The location id of observation
 * @param unique The unique users present during the interval
 */
case class LocStat(interval: Integer, loc: String, unique: Long)

/**
 * Generate heatmap for population distribution from mobility data.
 *
 * Input format: (uid, time, loc)
 * Output format: (loc, interval, unique_users)
 */
object Heatmap extends Serializable{

  def draw(input: RDD[STPoint], interval: Integer = 3600): RDD[LocStat] = {

    // group logs by (interval, location)
    input.groupBy(p => (p.loc, p.time.toInt/interval))

      // count users for each tuple
      .map { case ((loc, interval), points) =>
        new LocStat(interval, loc, points.map(_.uid).toArray.distinct.size.toLong)
    }
  }
}
