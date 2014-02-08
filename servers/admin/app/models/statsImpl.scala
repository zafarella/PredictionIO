package models

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

}
