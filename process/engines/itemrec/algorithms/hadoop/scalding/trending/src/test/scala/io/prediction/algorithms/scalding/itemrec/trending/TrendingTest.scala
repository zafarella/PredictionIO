package io.prediction.algorithms.scalding.itemrec.trending

import org.specs2.mutable._

import com.twitter.scalding._

import io.prediction.commons.filepath.{ DataFile, AlgoFile }

class TrendingTest extends Specification with TupleConversions {

  // helper function
  // only compare double up to 9 decimal places
  def roundingData(orgList: List[(String, Double)]) = {
    orgList map { x =>
      val (t1, t2) = x

      // NOTE: use HALF_UP mode to avoid error caused by rounding when compare data
      // (eg. 3.5 vs 3.499999999999).
      // (eg. 0.6666666666 vs 0.666666667)

      (t1, BigDecimal(t2).setScale(9, BigDecimal.RoundingMode.HALF_UP).toDouble)
    }
  }

  def test(testArgs: Map[String, String],
    testInput: List[(String, String)], // only checking relative order here
    testOutput: List[(String, Double)]) = {

    val appid = 1
    val engineid = 2
    val algoid = 3
    val hdfsRoot = "testroot/"

    JobTest("io.prediction.algorithms.scalding.itemrec.trending.Trending")
      .arg("appid", appid.toString)
      .arg("engineid", engineid.toString)
      .arg("algoid", algoid.toString)
      .arg("hdfsRoot", hdfsRoot)
      .arg("filter", testArgs("filter"))
      .arg("filterType", testArgs("filterType"))
      .arg("forecastModel", testArgs("forecastModel"))
      .arg("scoreType", testArgs("scoreType"))
      .arg("windowSize", testArgs("windowSize"))
      .source(Tsv(DataFile(hdfsRoot, appid, engineid, algoid, None, "ratings.tsv")), testInput)
      .sink[(String, Double)](Tsv(AlgoFile(hdfsRoot, appid, engineid, algoid, None, "itemRecScores.tsv"))) { outputBuffer =>
        "correctly calculate itemRecScores" in {
          val zipped = outputBuffer.sortBy(_._2) zip testOutput
          var same = true
          println(zipped)
          zipped.foreach { items =>
            val ((item1, score1), (item2, score2)) = items
            same &= (item1 == item2)
          }
          same
          // roundingData(outputBuffer.toList) must containTheSameElementsAs(roundingData(testOutput))
        }
      }
      .run
      .finish
  }

  // test1
  val test1args = Map[String, String]("filter" -> "false",
    "filterType" -> "nofilter",
    "forecastModel" -> "doubleExponential",
    "scoreType" -> "velocity",
    "windowSize" -> "hour"
  )

  val test1Input = List(
    ("i0", "0,1"),
    ("i1", "0,2"),
    ("i2", "0,3"),
    ("i3", "0,4"))

  val test1Output = List[(String, Double)](
    ("i0", 1),
    ("i1", 2),
    ("i2", 3),
    ("i3", 4))

  "Trending" should {
    test(test1args, test1Input, test1Output)
  }

}