package io.prediction.scheduler

import io.prediction.commons.Config
import play.api.mvc._
import play.api.libs.json._
import scala.collection.JavaConverters._
import org.hyperic.sigar._
import org.hyperic.sigar.ptql._

// Hadoop -> Use metrics2 api?
// Mongodb -> Use command line + javascript script

object StatsMonitor extends Controller {
  val sigar = new Sigar
  val pids = getPIDs
  val config = new Config
  
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
    
    //TODO Implement disk usage for MongoDB
    
    Ok(Json.obj("disk" -> (total)))
  }

}
