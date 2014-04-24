package io.prediction.scheduler

import io.prediction.commons.Config
import play.api.mvc._
import play.api.libs.json._
import play.api.Logger
import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.sys.process._
import org.hyperic.sigar._
import org.hyperic.sigar.ptql._

trait StatsMonitor {
  this: Controller =>

  val sigar = new Sigar
  val mem = new ProcMem
  val cpu = new ProcCpu
  val dir = new DirUsage

  val pids = getPIDs
  var config = new Config()

  val hosts = Seq(
    config.appdataDbHost + ":" + config.appdataDbPort + "/" + config.appdataDbName,
    config.appdataTrainingDbHost + ":" + config.appdataTrainingDbPort + "/" + config.appdataTrainingDbName,
    config.appdataValidationDbHost + ":" + config.appdataValidationDbPort + "/" + config.appdataValidationDbName,
    config.modeldataDbHost + ":" + config.modeldataDbPort + "/" + config.modeldataDbName,
    config.modeldataTrainingDbHost + ":" + config.modeldataTrainingDbPort + "/" + config.modeldataTrainingDbName,
    config.settingsDbHost + ":" + config.settingsDbPort + "/" + config.settingsDbName
  ) distinct

  val hostCmds = hosts map {
    host => Seq("mongo", host, "--eval", "printjson(db.stats())")
  }

  def getPIDs: Array[Long] = {
    if (config == null) {
      config = new Config()
    }

    val procFinder = new ProcessFinder(sigar)
    var pids = Array[Long]()

    //Get Java PIDs
    pids = pids ++ procFinder.find("State.Name.ct=java")

    //Get MongoDB PIDs
    if (config.settingsDbType == "mongodb") {
      pids = pids ++ procFinder.find("State.Name.ct=mongo")
    }

    return pids
  }

  private def getRam: Double = {
    var total = 0.0

    for (pid <- pids) {
      mem.gather(sigar, pid)
      total += mem.getSize;
    }

    return total
  }

  private def getCpu: Double = {
    var total = 0.0

    for (pid <- pids) {
      cpu.gather(sigar, pid)
      total += cpu.getPercent()
    }

    return total
  }

  def getMongoDisk: Double = {
    def getFileSize(hostCmd: Seq[String]) = {
      val output = hostCmd.!!

      val reg = """"fileSize\" : (\d+),""".r
      val size: String = reg findFirstIn output match {
        case Some(reg(s)) => s
        case None => "0"
      }

      size.toDouble
    }
    val fileSizes = hostCmds map getFileSize

    return fileSizes reduce (_ + _)
  }

  /**
   * Get total Disk Space used
   */
  private def getDisk: Double = {
    if (config == null) {
      config = new Config()
    }
    var total = 0.0

    //HDFS Disk Usage
    var hdfsDir = config.settingsHdfsRoot
    try {
      dir.gather(sigar, hdfsDir)
      total += dir.getDiskUsage()
    } catch {
      case e: Exception => Logger.warn("Could not get HDFS Directory at " + hdfsDir)
    }

    //Disk usage for MongoDB
    if (config.settingsDbType == "mongodb") {
      total += getMongoDisk
    }

    return total
  }

  /**
   * Get RAM usage in percent
   */
  def getRamUsage = Action {
    val total = getRam

    Ok(Json.obj("ram" -> (total)))
  }

  /**
   * Get CPU usage in percent
   */
  def getCpuUsage = Action {
    val total = getCpu

    Ok(Json.obj("cpu" -> (total)))
  }

  /**
   * Get total Disk Space used
   */
  def getUsedDiskSpace() = Action {
    val total = getDisk

    Ok(Json.obj("disk" -> (total)))
  }

  def getStats() = Action {
    val ram = getRam
    val cpu = getCpu
    val disk = getDisk

    Ok(Json.obj("ram" -> ram, "cpu" -> cpu, "disk" -> disk))
  }

}

object StatsMonitor extends Controller with StatsMonitor
