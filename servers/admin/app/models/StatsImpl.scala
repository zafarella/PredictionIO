package models

import io.prediction.commons.Config
import java.io.File
import org.hyperic.sigar._

object StatsImpl extends StatsMonitor {

  val sigar = new Sigar()
  val config = new Config()

  def getRamUsage(): Double = {
    val ram = new Mem()
    ram.gather(sigar)
    return ram.getUsedPercent()
  }

  def getCpuUsage(): Double = {
    val cpu = sigar.getCpuPerc()
    var sysUsage = cpu.getSys()
    var userUsage = cpu.getUser()
    var totalUsage = sysUsage + userUsage
    return totalUsage
  }

  def getProcessCpuUsage(pid: Long): Double = {
    var pc = new ProcCpu()
    pc.gather(sigar, pid)

    return pc.getPercent()
  }
  def getProcessRamUsage(pid: Long): Double = {
    var pm = new ProcMem()
    pm.gather(sigar, pid)

    return pm.getSize()
  }

  def getTotalDiskSpace(): Long = {
    var roots = File.listRoots()
    var totalSpace: Long = 0

    for (root <- roots) {
      totalSpace = totalSpace + root.getTotalSpace()
    }

    return totalSpace
  }

  def getUsedDiskSpace(): Long = {
    var hdfsDir = config.settingsHdfsRoot
    var d = new DirUsage()
    d.gather(sigar, hdfsDir)

    // TODO: Get disk space for mongodb

    return d.getDiskUsage()
  }

  def getFreeDiskSpace(): Long = {
    var roots = File.listRoots()
    var freeSpace: Long = 0

    for (root <- roots) {
      freeSpace = freeSpace + root.getFreeSpace()
    }

    return freeSpace
  }
}
