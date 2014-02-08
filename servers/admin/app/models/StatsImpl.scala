package models

import io.prediction.commons.Config
import java.io.File
import org.hyperic.sigar._

class StatsImpl extends StatsMonitor {
	
	val sigar = new Sigar()

	def getRamUsage() {
		val ram = new Mem(sigar)
		ram.gather()
		return ram.getUsedPercent()
	}

	def getCpuUsage() {
		val cpu = sigar.getCpuPerc()
		var sysUsage = cpu.getSys()
		var userUsage = cpu.getUser()
		var totalUsage = sysUsage + userUsage
		return totalUsage
	}

  def getTotalDiskSpace() {
    var roots = File.listRoots()
    var totalSpace = 0

    for (root <- roos) {
      totalSpace += root.getTotalSpace()
    }

    return totalSpace
  }

  def getUsedDiskSpace() {
    var hdfsDir = Config.settingsHdfsRoot
    var d = new DirUsage()
    d.gather(sigar, hdfsDir)

    // TODO: Get disk space for mongodb

    return d.getDiskUsage()
  }

  def getFreeDiskSpace() {
    var roots = File.listRoots()
    var freeSpace = 0

    for (root <- roots) {
      freeSpace += root.getFreeSpace()
    }

    return freeSpace
  }
}
