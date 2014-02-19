package io.prediction.scheduler

import play.api.mvc._

object StatsMonitor extends Controller {

  /**
   * Get RAM usage in percent
   */
  def getRamUsage = TODO

  /**
   * Get CPU usage in percent
   */
  def getCpuUsage = TODO

  /**
   * Get total Disk Space
   */
  def getTotalDiskSpace() = TODO

  /**
   * Get total Disk Space used
   */
  def getUsedDiskSpace() = TODO

  /**
   * Get total free Disk Space
   */
  def getFreeDiskSpace() = TODO
}
