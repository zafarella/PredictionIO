package io.prediction.tools.agent

import io.prediction.controller.Utils
import io.prediction.core.BuildInfo
import io.prediction.tools.ConsoleArgs

import grizzled.slf4j.Logging
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake
import org.json4s.JString
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._

import scala.sys.process._

import java.net.URI
import javax.net.ssl.SSLContext

class AgentWebSocketClient(uri: URI, pioHome: String)
    extends WebSocketClient(uri) with Logging {
  implicit val formats = Utils.json4sDefaultFormats

  override def onOpen(hs: ServerHandshake): Unit = {
    info(s"Connected to ${uri.toASCIIString}")
    val status = Agent.pioStatus(pioHome)
    info(status)
    send(status)
  }

  override def onMessage(msg: String): Unit = {
    val json = try {
      parse(msg)
    } catch {
      case e: Throwable =>
        error(e.getMessage)
        return
    }
    val event = (json \ "event").extractOpt[String]
    val action = (json \ "action").extractOpt[String]
    (event, action) match {
      case (Some("client:status"), Some("get")) =>
        val status = Agent.pioStatus(pioHome)
        info(status)
        send(status)
      case (Some("client:ping"), Some("get")) =>
        send(compact(render(Agent.createFrame(
          "client:ping", "set", JString("pong")))))
      case _ =>
        error(s"Unknown message received: ${event} ${action}")
    }
  }

  override def onClose(code: Int, reason: String, remote: Boolean): Unit = {
    info("Disconnected")
  }

  override def onError(e: Exception): Unit = {
    e.printStackTrace
  }
}

object Agent extends Logging {
  def createFrame(event: String, action: String, data: JValue): JValue =
    ("event" -> event) ~
    ("action" -> action) ~
    ("version" -> BuildInfo.version) ~
    ("data" -> data)

  def pioStatus(piohome: String) = {
    val status =
      Process(s"${piohome}/bin/pio status --json").lines_!.toList.last
    try {
      val statusJson = parse(status)
      compact(render(createFrame("client:status", "set", statusJson)))
    } catch {
      case e: Throwable => e.getMessage
    }
  }

  def start(ca: ConsoleArgs): Int = {
    val url = ca.common.agentUrl.getOrElse {
      sys.env.get("PIO_AGENT_URL").getOrElse {
        error("URL not found from configuration file nor command line. " +
          "Aborting.")
        return 1
      }
    }

    val hostname = ca.common.agentHostname.getOrElse {
      sys.env.get("PIO_AGENT_HOSTNAME").getOrElse {
        error("Hostname not found from configuration file nor command line. " +
          "Aborting.")
        return 1
      }
    }

    val secret = ca.common.agentSecret.getOrElse {
      sys.env.get("PIO_AGENT_SECRET_KEY").getOrElse {
        error("Secret key not found from configuration file nor command line." +
          " Aborting.")
        return 1
      }
    }

    val uri = new URI(
      s"${url}/api/socket?hostname=${hostname}&secret_key=${secret}&path=" +
      ca.common.pioHome.get)
    val agent = new AgentWebSocketClient(uri, ca.common.pioHome.get)
    if (uri.getScheme == "wss") {
      info("Enabling SSL")
      val sslContext = SSLContext.getInstance("TLS")
      sslContext.init(null, null, null)
      val factory = sslContext.getSocketFactory
      agent.setSocket(factory.createSocket)
    }
    agent.connectBlocking

    while (!agent.isClosed)
      Thread.sleep(1000)

    0
  }
}
