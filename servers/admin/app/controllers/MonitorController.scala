package controllers

import io.prediction.commons.Config

import play.api.mvc._

object MonitorController extends Controller {
  val config = new Config()
  def Monitor = Action {
    Ok(views.html.monitor.monitor(config.settingsSchedulerUrl))
  }
}
