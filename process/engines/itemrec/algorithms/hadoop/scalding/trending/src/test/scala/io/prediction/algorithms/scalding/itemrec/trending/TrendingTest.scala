package io.prediction.algorithms.scalding.itemrec.trending

import org.specs2.mutable._

import com.twitter.scalding._

import io.prediction.commons.filepath.{ DataFile, AlgoFile }

class TrendingTest extends Specification with TupleConversions {

  // helper function
  // only compare double up to 9 decimal places
  def roundingData(orgList: List[(String, String, Double)]) = {
    orgList map { x =>
      val (t1, t2, t3) = x

      // NOTE: use HALF_UP mode to avoid error caused by rounding when compare data
      // (eg. 3.5 vs 3.499999999999).
      // (eg. 0.6666666666 vs 0.666666667)

      (t1, t2, BigDecimal(t3).setScale(9, BigDecimal.RoundingMode.HALF_UP).toDouble)
    }
  }

  def test(testArgs: Map[String, String],
    testInput: List[(String, String, Int)],
    testOutput: List[(String, String, Double)]) = {

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
      .sink[(String, String, Double)](Tsv(AlgoFile(hdfsRoot, appid, engineid, algoid, None, "itemRecScores.tsv"))) { outputBuffer =>
        "correctly calculate itemRecScores" in {
          roundingData(outputBuffer.toList) must containTheSameElementsAs(roundingData(testOutput))
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
    ("u0", "i0", 1),
    ("u0", "i1", 2),
    ("u0", "i2", 3),
    ("u1", "i1", 4),
    ("u1", "i2", 4),
    ("u1", "i3", 2),
    ("u2", "i0", 3),
    ("u2", "i1", 2),
    ("u2", "i3", 1),
    ("u3", "i0", 2),
    ("u3", "i2", 1),
    ("u3", "i3", 5))

  val test1ItemSimScore = List(
    ("i0", "i1", 0.0),
    ("i1", "i0", 0.0),
    ("i0", "i2", -0.16666666666666666),
    ("i2", "i0", -0.16666666666666666),
    ("i0", "i3", -0.16666666666666666),
    ("i3", "i0", -0.16666666666666666),
    ("i1", "i2", 0.16666666666666666),
    ("i2", "i1", 0.16666666666666666),
    ("i1", "i3", 0.16666666666666666),
    ("i3", "i1", 0.16666666666666666),
    ("i2", "i3", -0.16666666666666666),
    ("i3", "i2", -0.16666666666666666))

  val test1Output = List[(String, String, Double)](
    ("u0", "i0", 1),
    ("u0", "i1", 2),
    ("u0", "i2", 3),
    ("u0", "i3", -0.666666666666667),
    ("u1", "i0", -3.0),
    ("u1", "i1", 4),
    ("u1", "i2", 4),
    ("u1", "i3", 2),
    ("u2", "i0", 3),
    ("u2", "i1", 2),
    ("u2", "i2", -0.666666666666667),
    ("u2", "i3", 1),
    ("u3", "i0", 2),
    ("u3", "i1", 3.0),
    ("u3", "i2", 1),
    ("u3", "i3", 5))

  "Trending" should {
    test(test1args, test1Input, test1Output)
  }

}