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

    request onSuccess {
      case res => Ok(views.html.monitor.monitor(res[0], res[1], res[2]))
    }
  }
}
