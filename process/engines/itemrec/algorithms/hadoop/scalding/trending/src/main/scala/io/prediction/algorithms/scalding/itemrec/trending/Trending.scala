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
 * --filter: <boolean> whether or not to filter the data
 * --filterType: <string> the type of filter to use on the timeseries
 * --forecastModel: <string> the type of forecasting model to use
 * --scoreType: <string> the type of scoring we should use
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

  val filterArg = args.getOrElse("filter", "false").toBoolean
  val filterTypeArg = args.getOrElse("filterType", "nofilter")
  val forecastModelArg = args("forecastModel")
  val scoreTypeArg = args("scoreType")
  val windowSizeArg = args("windowSize")

  val alpha = 0.5
  val beta = 0.5
  val gamma = 0.5
  val period = windowSizeArg match {
    case "hour" => 24
    case "day" => 7
    case "week" => 4
    case _ => -1
  }
  val numForecasts = period

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
      val forecastModel = forecastModelArg match {
        case "doubleExponential" => new DoubleExponentialModel(alpha, gamma)
        case "tripleExponential" => new HoltWintersModel(alpha, beta, gamma, period)
      }
      val forecasts = forecastModel.forecast(filtered, numForecasts)

      // now score the data
      val degree = scoreTypeArg match {
        case "velocity" => 1
        case "acceleration" => 2
      }
      var polyModel = new PolynomialModel(degree)
      var score = polyModel.getCoefficients(forecasts)(degree)
      Math.abs(score)
  }
    .project('iid, 'score)
    .write(itemRecScores)

}