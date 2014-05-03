package io.prediction.scheduler

import play.api.Play.current
import io.prediction.commons.Config
import java.io.File
import play.api.mvc._
import com.mongodb.casbah.Imports._
import play.api.libs.json._
import StatsMonitor._
import play.Logger

object StatsCollector extends Controller {

  //MongoDB information
  val MongoHost = "localhost"
  val MongoPort = 27017
  val DBname = "stats"
  val CollName = "stats"
  val Refresh = 2000 //Number of milli-seconds to refresh stats in

  //MongoDB objects

  val mongoClient = MongoClient(MongoHost, MongoPort)
  val db = mongoClient(DBname)
  val coll = db(CollName)

  /*
  Stats collector format:
  {
    timestamp : 
    cpuUsage : 
    ramUsage :
    diskUsage : 
  }
  */

  def getMongoObject(timestamp: Long, cpuUsage: Double, ramUsage: Double, diskUsage: Double) = {

    MongoDBObject(
      "timestamp" -> timestamp,
      "cpuUsage" -> cpuUsage,
      "ramUsage" -> ramUsage,
      "diskUsage" -> diskUsage
    )

  }

  //Function runs continuously and stores data in the given format
  def storeStats() {
    Logger.info("Storing Stats Collector")
    while (true) {
      try {
        val timestamp = System.currentTimeMillis / 1000

        //TODO: Get the following data from the API functions
        val cpuUsage = getCpu
        val ramUsage = getRam
        val diskUsage = getDisk

        val dbentry = getMongoObject(timestamp, cpuUsage, ramUsage, diskUsage)
        coll.insert(dbentry)
      } catch {
        case e: Exception => {
          Logger.error("Error inserting Stat to DB... Continuing...")
        }
      }

      Thread sleep Refresh
    }
  }

  def getStats(n: Int) = Action {
    val order = MongoDBObject("timestamp" -> -1)
    val stats = coll.find().sort(order).limit(n)
    val json = "[%s]".format(
      stats.toList.mkString(",")
    )

    Ok(json).as("application/json")
  }

}
