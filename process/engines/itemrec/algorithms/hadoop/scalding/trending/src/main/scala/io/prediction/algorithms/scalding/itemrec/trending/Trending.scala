package io.prediction.algorithms.scalding.itemrec.knnitembased

import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix
import io.prediction.commons.filepath.{ DataFile, AlgoFile }

import cascading.pipe.Pipe

/**
 * Source: ratings.tsv
 * Sink: itemRecScores.tsv
 * Descripton:
 *   predict and compute the scores of every predictable user-item pair.
 *
 * Required args:
 * --hdfsRoot: <string>. Root directory of the HDFS
 *
 * --appid: <int>
 * --engineid: <int>
 * --algoid: <int>
 *
 * --numRecommendations: <int>. number of recommendations to be generated
 *
 * Optional args:
 * --evalid: <int>. Offline Evaluation if evalid is specified
 * --mergeRatingParam: If defined, merge known rating into final predicted scores
 *
 */
class Trending(args: Args) extends Job(args) {

  // args
  val hdfsRootArg = args("hdfsRoot")

  val appidArg = args("appid").toInt
  val engineidArg = args("engineid").toInt
  val algoidArg = args("algoid").toInt
  val evalidArg = args.optional("evalid") map (x => x.toInt)

  val numRecommendationsArg = args("numRecommendations").toInt

  val ratingsRaw = Tsv(DataFile(hdfsRootArg, appidArg, engineidArg, algoidArg, evalidArg, "ratings.tsv")).read
  val itemRecScores = Tsv(AlgoFile(hdfsRootArg, appidArg, engineidArg, algoidArg, evalidArg, "itemRecScores.tsv"))

  // start computation  
  val ratings = ratingsRaw.mapTo((0, 1) -> ('iid, 'timeseries)) {
    fields: (String, String) =>
      val (iid, timeseriesstring) = fields
      (iid, timeseriesstring.split(",").map(_.toInt))
  }

  ratings.map('timeseries -> 'predictions) {
    timeseries: Array[Int] =>
      null
  }.write(itemRecScores)

}