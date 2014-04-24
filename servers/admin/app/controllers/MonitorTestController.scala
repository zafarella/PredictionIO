package controllers

import play.api.mvc._
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.json._
import play.api.libs.ws.WS
import Application.settingsSchedulerUrl

class MonitorTestController extends Controller {
  def testMonitor = Action {
    val request = for {
      cpu <- WS.url(s"${settingsSchedulerUrl}/monitor/cpuUsage").get()
      ram <- WS.url(s"${settingsSchedulerUrl}/monitor/ramUsage").get()
      disk <- WS.url(s"${settingsSchedulerUrl}/monitor/usedDiskUsage").get()
    } yield List(cpu, ram, disk)

    var c = 0.0
    var r = 0.0
    var d = 0.0

    request onSuccess {
      case cpu :: ram :: disk :: List() =>
        c += (cpu.json \ "cpu").validate[Double].get
        r += (cpu.json \ "cpu").validate[Double].get
        d += (cpu.json \ "cpu").validate[Double].get
    }
    /*
       Ok(views.html.monitor.monitor(
          (cpu.json \ "cpu").validate[Double].get, 
          (ram.json \ "ram").validate[Double].get, 
          (disk.json \ "disk").validate[Double].get
          )
        )*/

    Ok(views.html.monitor.monitor(c, r, d))
  }
}
