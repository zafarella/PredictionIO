package controllers

import io.prediction.commons.Config

import play.api.mvc._
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.json._
import play.api.libs.ws.WS
import Application.settingsSchedulerUrl

class MonitorTestController extends Controller {
  val config = new Config()
  def testMonitor = Action {
    Ok(views.html.monitor.monitor(config.settingsSchedulerUrl))
  }
}
