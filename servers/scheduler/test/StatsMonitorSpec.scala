package io.prediction.scheduler

import org.specs2.mock._
import org.specs2.mutable._
import play.api.libs.json._
import play.api.mvc._
import play.api.test._
import scala.collection.Seq
import scala.concurrent.Future
import scala.sys.process._
import org.hyperic.sigar._

object StatsMonitorSpec extends PlaySpecification with Results with Mockito {

  class TestStatsMonitor() extends Controller with StatsMonitor

  val mockPids = Array(0L, 1L, 2L)
  val mockSigar = null

  "StatsMonitor#getRamUsage" should {
    "should return" in {
      val mockMem = spy[ProcMem]
      mockMem.gather(===(mockSigar), anyLong) returns Unit
      mockMem.getSize returns 2L

      val controller = new TestStatsMonitor() {
        override val sigar = mockSigar
        override val pids = mockPids
        override val mem = mockMem
      }

      val result: Future[SimpleResult] = controller.getRamUsage.apply(FakeRequest())
      val bodyJson = Json.parse(contentAsString(result))

      Json.stringify(bodyJson \ "ram") must be equalTo "6.0"
    }
  }

  "StatsMonitor#getCpuUsage" should {
    "should return" in {
      val mockCpu = spy[ProcCpu]
      mockCpu.gather(===(mockSigar), anyLong) returns Unit
      mockCpu.getPercent returns 12.3

      val controller = new TestStatsMonitor() {
        override val sigar = mockSigar
        override val pids = mockPids
        override val cpu = mockCpu
      }

      val result: Future[SimpleResult] = controller.getCpuUsage.apply(FakeRequest())
      val bodyJson = Json.parse(contentAsString(result))

      Json.stringify(bodyJson \ "cpu") must be equalTo "36.9"
    }
  }

  "StatsMonitor#getDiskUsage" should {
    "should return" in {
      val mockDir = mock[DirUsage]
      mockDir.gather(===(mockSigar), anyString) returns Unit
      mockDir.getDiskUsage() returns 12L

      val mockMongo0 = Seq("0")
      val mockMongo1 = Seq("1")
      val mockMongo2 = Seq("2")

      mockMongo0.!! returns mockMongoResult(5)
      mockMongo1.!! returns mockMongoResult(10)
      mockMongo2.!! returns mockMongoResult(20)

      val controller = new TestStatsMonitor() {
        override val sigar = mockSigar
        override val pids = mockPids
        override val dir = mockDir
        override val hostCmds = Seq(mockMongo0, mockMongo1, mockMongo2)
      }

      val result: Future[SimpleResult] = controller.getUsedDiskSpace.apply(FakeRequest())
      val bodyJson = Json.parse(contentAsString(result))

      Json.stringify(bodyJson \ "disk") must be equalTo "47.0"
    }
  }

  def mockMongoResult(size: Int): String = {
    return "filesize    :        " + size
  }
}
