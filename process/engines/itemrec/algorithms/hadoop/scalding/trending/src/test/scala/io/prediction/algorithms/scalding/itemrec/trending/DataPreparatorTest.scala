package io.prediction.algorithms.scalding.itemrec.trending

import org.specs2.mutable._

import com.twitter.scalding._

import io.prediction.commons.scalding.appdata.{ Items, U2iActions }
import io.prediction.commons.filepath.DataFile

class DataPreparatorTest extends Specification with TupleConversions {

  val rate = "rate"
  val like = "like"
  val dislike = "dislike"
  val view = "view"
  val viewDetails = "viewDetails"
  val conversion = "conversion"

  val appid = 2
  val engineid = 4
  val algoid = 5

  def test(itypes: List[String], params: Map[String, String],
    items: List[(String, String)], u2iActions: List[(String, String, String, String, String)],
    timeseries: List[(String, String)], selectedItems: List[(String, String)]) = {

    val dbType = "file"
    val dbName = "testpath/"
    val dbHost = None //Option("testhost")
    val dbPort = None //Option(27017)
    val hdfsRoot = "testroot/"

    JobTest("io.prediction.algorithms.scalding.itemrec.trending.DataPreparator")
      .arg("dbType", dbType)
      .arg("dbName", dbName)
      .arg("hdfsRoot", hdfsRoot)
      .arg("appid", appid.toString)
      .arg("engineid", engineid.toString)
      .arg("algoid", algoid.toString)
      .arg("itypes", itypes)
      .arg("action", params("action"))
      .arg("endTime", params("endTime"))
      .arg("windowSize", params("windowSize"))
      .arg("numWindows", params("numWindows"))
      //.arg("debug", List("test")) // NOTE: test mode
      .source(Items(appId = appid, itypes = Some(itypes), dbType = dbType, dbName = dbName, dbHost = dbHost, dbPort = dbPort).getSource, items)
      .source(U2iActions(appId = appid, dbType = dbType, dbName = dbName, dbHost = dbHost, dbPort = dbPort).getSource, u2iActions)
      .sink[(String, String, Int)](Tsv(DataFile(hdfsRoot, appid, engineid, algoid, None, "ratings.tsv"))) { outputBuffer =>
        "correctly process and write data to ratings.tsv" in {
          outputBuffer.toList must containTheSameElementsAs(timeseries)
        }
      }
      .sink[(String, String)](Tsv(DataFile(hdfsRoot, appid, engineid, algoid, None, "selectedItems.tsv"))) { outputBuffer =>
        "correctly write selectedItems.tsv" in {
          outputBuffer.toList must containTheSameElementsAs(selectedItems)
        }
      }
      .run
      .finish

  }

  /** no itypes specified */
  def testWithoutItypes(params: Map[String, String],
    items: List[(String, String)], u2iActions: List[(String, String, String, String, String)],
    timeseries: List[(String, String)], selectedItems: List[(String, String)]) = {

    val dbType = "file"
    val dbName = "testpath/"
    val dbHost = None //Option("testhost")
    val dbPort = None //Option(27017)
    val hdfsRoot = "testroot/"

    JobTest("io.prediction.algorithms.scalding.itemrec.trending.DataPreparator")
      .arg("dbType", dbType)
      .arg("dbName", dbName)
      //.arg("dbHost", dbHost.get)
      //.arg("dbPort", dbPort.get.toString)
      .arg("hdfsRoot", hdfsRoot)
      .arg("appid", appid.toString)
      .arg("engineid", engineid.toString)
      .arg("algoid", algoid.toString)
      //.arg("itypes", itypes) // NOTE: no itypes args!
      .arg("action", params("action"))
      .arg("endTime", params("endTime"))
      .arg("windowSize", params("windowSize"))
      .arg("numWindows", params("numWindows"))
      //.arg("debug", List("test")) // NOTE: test mode
      .source(Items(appId = appid, itypes = None, dbType = dbType, dbName = dbName, dbHost = dbHost, dbPort = dbPort).getSource, items)
      .source(U2iActions(appId = appid, dbType = dbType, dbName = dbName, dbHost = dbHost, dbPort = dbPort).getSource, u2iActions)
      .sink[(String, String, Int)](Tsv(DataFile(hdfsRoot, appid, engineid, algoid, None, "ratings.tsv"))) { outputBuffer =>
        "correctly process and write data to ratings.tsv" in {
          outputBuffer.toList must containTheSameElementsAs(timeseries)
        }
      }
      .sink[(String, String)](Tsv(DataFile(hdfsRoot, appid, engineid, algoid, None, "selectedItems.tsv"))) { outputBuffer =>
        "correctly write selectedItems.tsv" in {
          outputBuffer.toList must containTheSameElementsAs(selectedItems)
        }
      }
      .run
      .finish

  }

  /**
   * Test 1. basic. view actions only
   */
  val test1AllItypes = List("t1", "t2", "t3", "t4")
  val test1Items = List(("i0", "t1,t2,t3"),
    ("i1", "t2,t3"),
    ("i2", "t4"),
    ("i3", "t3,t4"))
  val test1U2i = List(
    (view, "u0", "i0", "0", "3"),
    (view, "u0", "i1", "1", "1"),
    (view, "u0", "i2", "2", "4"),
    (view, "u0", "i3", "3", "2"),
    (view, "u1", "i0", "3", "5"),
    (view, "u1", "i1", "2", "2"),
    (view, "u1", "i2", "1", "1"),
    (view, "u1", "i3", "0", "2"))
  val test1Timeseries = List(
    ("i0", "1,0,0"),
    ("i1", "0,1,1"),
    ("i2", "0,1,1"),
    ("i3", "1,0,0"))
  val test1Params: Map[String, String] = Map(
    "action" -> view,
    "endTime" -> "3",
    "windowSize" -> "1",
    "numWindows" -> "3")

  "itemrec.trending DataPreparator with only view actions, all itypes" should {
    test(test1AllItypes, test1Params, test1Items, test1U2i, test1Timeseries, test1Items)
  }

  "itemrec.trending DataPreparator with only view actions, no itypes specified" should {
    testWithoutItypes(test1Params, test1Items, test1U2i, test1Timeseries, test1Items)
  }

}
