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

  def test(itypes: List[String], params: Map[String, String],
    items: List[(String, String)], u2iActions: List[(String, String, String, String, String)],
    ratings: List[(String, String, Int)], selectedItems: List[(String, String)]) = {

    val dbType = "file"
    val dbName = "testpath/"
    val dbHost = None //Option("testhost")
    val dbPort = None //Option(27017)
    val hdfsRoot = "testroot/"

    JobTest("io.prediction.algorithms.scalding.itemrec.trending.DataPreparator")
      .arg("dbType", dbType)
      .arg("dbName", dbName)
      .arg("hdfsRoot", hdfsRoot)
      .arg("appid", "2")
      .arg("engineid", "4")
      .arg("algoid", "5")
      .arg("itypes", itypes)
      .arg("action", params("action"))
      .arg("startTime", params("startTime"))
      .arg("windowSize", params("windowSize"))
      .arg("numWindows", params("numWindows"))
      //.arg("debug", List("test")) // NOTE: test mode
      .source(Items(appId = 2, itypes = Some(itypes), dbType = dbType, dbName = dbName, dbHost = dbHost, dbPort = dbPort).getSource, items)
      .source(U2iActions(appId = 2, dbType = dbType, dbName = dbName, dbHost = dbHost, dbPort = dbPort).getSource, u2iActions)
      .sink[(String, String, Int)](Tsv(DataFile(hdfsRoot, 2, 4, 5, None, "ratings.tsv"))) { outputBuffer =>
        "correctly process and write data to ratings.tsv" in {
          outputBuffer.toList must containTheSameElementsAs(ratings)
        }
      }
      .sink[(String, String)](Tsv(DataFile(hdfsRoot, 2, 4, 5, None, "selectedItems.tsv"))) { outputBuffer =>
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
    ratings: List[(String, String, Int)], selectedItems: List[(String, String)]) = {

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
      .arg("appid", "2")
      .arg("engineid", "4")
      .arg("algoid", "5")
      //.arg("itypes", itypes) // NOTE: no itypes args!
      .arg("action", params("action"))
      .arg("startTime", params("startTime"))
      .arg("windowSize", params("windowSize"))
      .arg("numWindows", params("numWindows"))
      //.arg("debug", List("test")) // NOTE: test mode
      .source(Items(appId = 2, itypes = None, dbType = dbType, dbName = dbName, dbHost = dbHost, dbPort = dbPort).getSource, items)
      .source(U2iActions(appId = 2, dbType = dbType, dbName = dbName, dbHost = dbHost, dbPort = dbPort).getSource, u2iActions)
      .sink[(String, String, Int)](Tsv(DataFile(hdfsRoot, 2, 4, 5, None, "ratings.tsv"))) { outputBuffer =>
        "correctly process and write data to ratings.tsv" in {
          outputBuffer.toList must containTheSameElementsAs(ratings)
        }
      }
      .sink[(String, String)](Tsv(DataFile(hdfsRoot, 2, 4, 5, None, "selectedItems.tsv"))) { outputBuffer =>
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
  val test1Ratings = List(
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
    test(test1AllItypes, test1Params, test1Items, test1U2i, test1Ratings, test1Items)
  }

  "itemrec.trending DataPreparator with only view actions, no itypes specified" should {
    testWithoutItypes(test1Params, test1Items, test1U2i, test1Ratings, test1Items)
  }

}
