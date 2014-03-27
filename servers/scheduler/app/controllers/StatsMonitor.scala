package io.prediction.scheduler

import io.prediction.commons.Config
import play.api.mvc._
import play.api.libs.json._
import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.sys.process._
import org.hyperic.sigar._
import org.hyperic.sigar.ptql._

// Hadoop -> Use metrics2 api?
// Mongodb -> Use command line + javascript script

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

    //Get Java PIDs
    val java = procFinder.find("State.name.ct=java")

    //Get MongoDB PIDs
    val mongo = procFinder.find("State.name.ct=mongo")

    return mongo ++ java
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

  private def getDisk: Double = {
    var total = 0.0

    //HDFS Disk Usage
    var hdfsDir = config.settingsHdfsRoot
    dir.gather(sigar, hdfsDir)

    total += dir.getDiskUsage()

    //Disk usage for MongoDB
    def getFileSize(hostCmd: Seq[String]) = {
      val output = hostCmd.!!
      val reg = """\"fileSize\"(\s):(\s)(\d+),""".r
      val reg(a, b, size) = output

      size.toLong
    }
    val fileSizes = hostCmds map getFileSize
    total += fileSizes reduce (_ + _)

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
