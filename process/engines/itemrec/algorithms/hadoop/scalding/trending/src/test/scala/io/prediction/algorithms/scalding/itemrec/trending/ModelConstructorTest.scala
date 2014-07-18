package io.prediction.algorithms.scalding.itemrec.trending

import org.specs2.mutable._

import com.twitter.scalding._

import io.prediction.commons.scalding.appdata.Users
import io.prediction.commons.filepath.{ AlgoFile, DataFile }
import io.prediction.commons.scalding.modeldata.ItemRecScores

class ModelConstructorTest extends Specification with TupleConversions {

  def test(numRecommendations: Int,
    items: List[(String, String)], //iid, itypes
    itemRecScores: List[(String, String)],
    users: List[(String)], // uid
    output: List[(String, String, String, String)]) = {

    val appid = 3
    val engineid = 4
    val algoid = 7
    val modelSet = true

    val dbType = "file"
    val dbName = "testpath/"
    val dbHost = None
    val dbPort = None
    val hdfsRoot = "testroot/"

    val outputItemRecScores = output map { case (uid, iid, score, itypes) => (uid, iid, score, itypes, algoid, modelSet) }

    JobTest("io.prediction.algorithms.scalding.itemrec.trending.ModelConstructor")
      .arg("dbType", dbType)
      .arg("dbName", dbName)
      .arg("hdfsRoot", hdfsRoot)
      .arg("appid", appid.toString)
      .arg("engineid", engineid.toString)
      .arg("algoid", algoid.toString)
      .arg("modelSet", modelSet.toString)
      .arg("numRecommendations", numRecommendations.toString)
      //.arg("debug", "test") // NOTE: test mode
      .source(Users(appId = appid, dbType = dbType, dbName = dbName, dbHost = dbHost, dbPort = dbPort).getSource, users)
      .source(Tsv(AlgoFile(hdfsRoot, appid, engineid, algoid, None, "itemRecScores.tsv")), itemRecScores)
      .source(Tsv(DataFile(hdfsRoot, appid, engineid, algoid, None, "selectedItems.tsv")), items)
      .sink[(String, String, String, String, Int, Boolean)](ItemRecScores(dbType = dbType, dbName = dbName, dbHost = dbHost, dbPort = dbPort, algoid = algoid, modelset = modelSet).getSource) { outputBuffer =>
        "correctly write model data to a file" in {
          outputBuffer.toList must containTheSameElementsAs(outputItemRecScores)
        }
      }
      .run
      .finish
  }

  /* test 1 */
  val test1ItemRecScores = List(
    ("i0", "1.23"),
    ("i1", "0.123"),
    ("i2", "0.456"))
  val test1Items = List(
    ("i0", "t1,t2,t3"),
    ("i1", "t1,t2"),
    ("i2", "t2,t3"))
  val test1Users = List(
    ("u0"),
    ("u1"),
    ("u2"))
  val test1Output = List(
    ("u0", "i0,i2,i1", "1.23,0.456,0.123", "[t1,t2,t3],[t2,t3],[t1,t2]"),
    ("u1", "i0,i2,i1", "1.23,0.456,0.123", "[t1,t2,t3],[t2,t3],[t1,t2]"),
    ("u2", "i0,i2,i1", "1.23,0.456,0.123", "[t1,t2,t3],[t2,t3],[t1,t2]"))

  "itemrec.trending ModelConstructor" should {
    test(3, test1Items, test1ItemRecScores, test1Users, test1Output)
  }

  "itemrec.trending ModelConstructor" should {
    test(5, test1Items, test1ItemRecScores, test1Users, test1Output)
  }

  /* test 2 */
  val test2Output = List(
    ("u0", "i0,i2", "1.23,0.456", "[t1,t2,t3],[t2,t3]"),
    ("u1", "i0,i2", "1.23,0.456", "[t1,t2,t3],[t2,t3]"),
    ("u2", "i0,i2", "1.23,0.456", "[t1,t2,t3],[t2,t3]"))

  "itemrec.trending ModelConstructor" should {
    test(2, test1Items, test1ItemRecScores, test1Users, test2Output)
  }

}
