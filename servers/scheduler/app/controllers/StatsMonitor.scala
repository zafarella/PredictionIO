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

object StatsMonitor extends Controller {
  val sigar = new Sigar
  val pids = getPIDs
  val config = new Config
  val hosts = Seq(
        config.appdataDbHost+":"+config.appdataDbPort+"/"+config.appdataDbName,
        config.appdataTrainingDbHost+":"+config.appdataTrainingDbPort+"/"+config.appdataTrainingDbName,
        config.appdataValidationDbHost+":"+config.appdataValidationDbPort+"/"+config.appdataValidationDbName,
        config.modeldataDbHost+":"+config.modeldataDbPort+"/"+config.modeldataDbName,
        config.modeldataTrainingDbHost+":"+config.modeldataTrainingDbPort+"/"+config.modeldataTrainingDbName,
        config.settingsDbHost+":"+config.settingsDbPort+"/"+config.settingsDbName
        ) distinct
  
  def getPIDs: Array[Long] = {
    val procFinder = new ProcessFinder(sigar)

    //Get Java PIDs
    val java = procFinder.find("State.name.ct=java")
    
    //Get MongoDB PIDs
    val mongo = procFinder.find("State.name.ct=mongo")
    
    return mongo ++ java
  }

  /**
   * Get RAM usage in percent
   */
  def getRamUsage = Action {
    val mem = new ProcMem
    var total = 0.0
    
    for(pid <- pids) {
      mem.gather(sigar, pid)
      total += mem.getSize;
    }
    
    Ok(Json.obj("ram" -> (total)))
  }

  /**
   * Get CPU usage in percent
   */
  def getCpuUsage = Action {
    val cpu = new ProcCpu
    var total = 0.0
    
    for(pid <- pids) {
      cpu.gather(sigar, pid)
      total += cpu.getPercent()
    }
    
    
    Ok(Json.obj("cpu" -> (total)))
  }

  /**
   * Get total Disk Space used
   */
  def getUsedDiskSpace() = Action {
    var total = 0.0
    
    //HDFS Disk Usage
    var hdfsDir = config.settingsHdfsRoot
    var d = new DirUsage()
    d.gather(sigar, hdfsDir)
    
    total += d.getDiskUsage()
    
    //Disk usage for MongoDB
    def getFileSize(host:String) = {
      val output = Seq("mongo", host, "--eval", "'printjson(db.stats())'").!!
      val reg = """\"fileSize\"(\s):(\s)(\d+),""".r
      val reg(a, b, size) = output
      
      size.toLong
    }
    val fileSizes = hosts map getFileSize
    total += fileSizes reduce ( _ + _ )
    
    Ok(Json.obj("disk" -> (total)))
  }

}
