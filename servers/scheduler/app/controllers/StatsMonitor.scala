package io.prediction.scheduler

import io.prediction.commons.Config
import play.api.mvc._
import play.api.libs.json._
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
  val config = new Config
  val hosts = Seq(
    config.appdataDbHost + ":" + config.appdataDbPort + "/" + config.appdataDbName,
    config.appdataTrainingDbHost + ":" + config.appdataTrainingDbPort + "/" + config.appdataTrainingDbName,
    config.appdataValidationDbHost + ":" + config.appdataValidationDbPort + "/" + config.appdataValidationDbName,
    config.modeldataDbHost + ":" + config.modeldataDbPort + "/" + config.modeldataDbName,
    config.modeldataTrainingDbHost + ":" + config.modeldataTrainingDbPort + "/" + config.modeldataTrainingDbName,
    config.settingsDbHost + ":" + config.settingsDbPort + "/" + config.settingsDbName
  ) distinct

  val hostCmds = hosts map {
    host => Seq("mongo", host, "--eval", "'printjson(db.stats())'")
  }

  def getPIDs: Array[Long] = {
    val procFinder = new ProcessFinder(sigar)
    var pids = Array[Long]()

    //Get Java PIDs
    pids = pids ++ procFinder.find("State.name.ct=java")

    //Get MongoDB PIDs
    if (config.settingsDbTyep == "mongodb") {
      pids =  pids ++ procFinder.find("State.name.ct=mongo")
    }

    return pids
  }

  /**
   * Get RAM usage in percent
   */
  def getRamUsage = Action {
    var total = 0.0

    for (pid <- pids) {
      mem.gather(sigar, pid)
      total += mem.getSize;
    }

    Ok(Json.obj("ram" -> (total)))
  }

  /**
   * Get CPU usage in percent
   */
  def getCpuUsage = Action {
    var total = 0.0

    for (pid <- pids) {
      cpu.gather(sigar, pid)
      total += cpu.getPercent()
    }

    Ok(Json.obj("cpu" -> (total)))
  }

  def getMongoDisk : Long = {
    def getFileSize(hostCmd: String) = {
      val output = hostCmd.!!
      val reg = """\"fileSize\"(\s):(\s)(\d+),""".r
      val reg(a, b, size) = output

      size.toLong
    }
    val fileSizes = hostCmds map getFileSize

    return fileSizes reduce (_ + _)
  }

  /**
   * Get total Disk Space used
   */
  def getUsedDiskSpace() = Action {
    var total = 0.0

    //HDFS Disk Usage
    var hdfsDir = config.settingsHdfsRoot
    dir.gather(sigar, hdfsDir)

    total += dir.getDiskUsage()

    //Disk usage for MongoDB
    if (config.settingsDbType == "mongodb") {
      total += getMongoDisk
    }

    Ok(Json.obj("disk" -> (total)))
  }

}

object StatsMonitor extends Controller with StatsMonitor
