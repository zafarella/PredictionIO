package io.prediction.algorithms.scalding.itemrec.trending

import com.twitter.scalding._
import breeze.linalg._
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
  val filterArg = args("filter").toBoolean
  val filterTypeArg = args("filterType")
  val forecastTypeArg = args("forecastType")
  val numForecastsArg = args("numForecasts").toInt
  val scoreTypeArg = args("scoreType")

  val ratingsRaw = Tsv(DataFile(hdfsRootArg, appidArg, engineidArg, algoidArg, evalidArg, "ratings.tsv")).read
  val itemRecScores = Tsv(AlgoFile(hdfsRootArg, appidArg, engineidArg, algoidArg, evalidArg, "itemRecScores.tsv"))

  // start computation
  val ratings = ratingsRaw.mapTo((0, 1) -> ('iid, 'timeseries)) {
    fields: (String, String) =>
      val (iid, timeseriesstring) = fields
      (iid, timeseriesstring.split(",").map(_.toInt))
  }

  ratings.map('timeseries -> 'score) {
    timeseries: Array[Int] =>
      // do any filtering/smoothing
      val timeseriesVec = DenseVector[Double](timeseries.map(_.toDouble))
      val filtered = filterTypeArg match {
        case "movingAverage" => timeseriesVec
        case "savitzkyGolay" => new SavitzkyGolayFilter(5).filter(timeseriesVec)
        case _ => timeseriesVec
      }

      // make forcasts
      val alpha = 0.5 // TODO what are we going to do for these?
      val gamma = 0.5
      val beta = 0.5
      val period = 10
      val forecastModel = forecastTypeArg match {
        case "doubleExponential" => new DoubleExponentialModel(alpha, gamma)
        case "tripleExponential" => new HoltWintersModel(alpha, beta, gamma, period)
      }
      val forecasts = forecastModel.forecast(filtered, numForecastsArg)

      // now score the data
      val degree = scoreTypeArg match {
        case "velocity" => 1
        case "acceleration" => 2
      }
      var polyModel = new PolynomialModel(degree)
      var score = polyModel.getCoefficients(forecasts)(degree)
      score -= polyModel.getCoefficients(filtered)(degree)
      Math.abs(score)
  }
    .project('iid, 'score)
    .write(itemRecScores)

}