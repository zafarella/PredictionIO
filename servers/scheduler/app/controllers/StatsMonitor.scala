package io.prediction.scheduler

import play.api.mvc._

// Hadoop -> Use metrics2 api
// Mongodb -> Use command line + javascript script

object StatsMonitor extends Controller {

  /**
   * Get RAM usage in percent
   */
  def getRamUsage = TODO

  /**
   * Get CPU usage in percent
   */
  def getCpuUsage = TODO
  //NOTE: Currently only monitors the java processes in the scheduler server
  

  /**
   * Get total Disk Space
   */
  def getTotalDiskSpace() = TODO
  // Needed?

  /**
   * Get total Disk Space used
   */
  def getUsedDiskSpace() = TODO

  /**
   * Get total free Disk Space
   */
  def getFreeDiskSpace() = TODO
  // Needed?
  
}
