package io.prediction.algorithms.scalding.itemrec.knntrend

import com.twitter.scalding._
import scala.List
import scala.util.parsing.combinator._

/**
 * Created by vincent on 2/8/14.
 * Based on the work of Stanislav Nikolov on how to detect trends using machine learning
 * http://snikolov.wordpress.com/2012/11/14/early-detection-of-twitter-trends/
 *
 * data_length : data length to test at the end of each time series
 * time : time at which we want to find trending items (eventually simply the last item of each series)
 * influence : influence radius of each sample (usually between 0 and 1. Will accept greater distances with a factor closer to 0)
 *            to know which value to use : 1 / maximum radius
 * samples : directory of the samples
 */
class KnnTrend(args: Args) extends Job(args) {

  object DoubleListParser extends RegexParsers {
    def apply(s: String) = parse("List(" ~> repsep("\\d+(\\.\\d*)".r ^^ (_.toDouble), ",") <~ ")", s)
  }

  ///////////////////////////////////////////////////////
  //               ALGO PARAMS
  ///////////////////////////////////////////////////////
  val data_length = args("data_length").toInt
  val t = args("time").toInt
  val y = args("influence").toDouble;
  val trending_samples_path = args("trending_samples");
  val not_trending_samples_path = args("not_trending_samples");
  val input_path = args("input");
  val output_path = args("output");

  ///////////////////////////////////////////////////////
  //               Read Input
  ///////////////////////////////////////////////////////

  val input = TextLine(input_path)
  //Read the file
  val time_series =
    input.mapTo('id, 'observations) {
      line: String =>
        val raw = line.split("\t").toList

        val id = raw(0)
        val values: List[Double] = raw.drop(1).map(_.toDouble).slice(t - data_length, t)

        (id, values)
    }

  val trending_samples = TextLine(trending_samples_path)
    .mapTo('samples) {
      line: String =>
        DoubleListParser(line).get
    }

  val not_trending_samples = TextLine(not_trending_samples_path)
    .mapTo('samples) {
      line: String =>
        DoubleListParser(line).get
    }

  ///////////////////////////////////////////////////////
  //          Get trending items
  ///////////////////////////////////////////////////////

  val trending_votes = time_series.crossWithTiny(trending_samples).mapTo(('id, 'samples, 'observations) -> ('id, 'trending_vote)) {
    x: (String, List[Double], List[Double]) =>
      val id = x._1
      val samples = x._2
      val observations = x._3

      (id, Distance(samples, observations))
  }.groupBy('id) {
    _.sum('trending_vote -> 'trending_votes)

  }

  val not_trending_votes = time_series.crossWithTiny(not_trending_samples).mapTo(('id, 'samples, 'observations) -> ('id, 'not_trending_vote)) {
    x: (String, List[Double], List[Double]) =>
      val id = x._1
      val samples = x._2
      val observations = x._3
      (id, Distance(samples, observations))
  }.groupBy('id) {
    _.sum('not_trending_vote -> 'not_trending_votes)
  }

  //Compute trending factor and get the top n trending items
  val results = trending_votes.joinWithSmaller('id -> 'id, not_trending_votes).mapTo(('id, 'trending_votes, 'not_trending_votes) -> ('id, 'trending_factor)) {
    x: (String, Double, Double) =>
      val id = x._1
      val trending_vote = x._2
      val not_trending_vote = x._3
      (id, trending_vote / not_trending_vote)
  }.groupAll {
    _.sortedReverseTake[(Double, String)](('trending_factor, 'id) -> 'top, 250)
  }.flattenTo[(Double, String)]('top -> ('trending_factor, 'id))
  //.limit(result_length)

  ///////////////////////////////////////////////////////
  //--------- Writing output
  ///////////////////////////////////////////////////////

  val output = Csv(p = output_path, separator = "\t")
  results /*.joinWithLarger('id -> 'id, time_series)
    .groupAll { _.sortBy('trending_factor) }
    */ .write(output)

  ///////////////////////////////////////////////////////
  //--------- Distance between two series
  ///////////////////////////////////////////////////////

  //In general, the examples s will be much longer than the observations o. In that case, we look for the “best match” between s and o and define the distance d(s,o)
  // to be the minimum distance over all dim(s)-sized chunks of o.
  def Distance(s: List[Double], o: List[Double]): Double = {
    var min_dist: Double = Int.MaxValue

    for (i <- 0 to s.length - o.length) {
      val dist: Double = SumEuclideanDistance(s.slice(i, i + o.length), o)

      min_dist = math.min(dist, min_dist)
    }
    return math.exp(-y * min_dist)
  }

  def SumEuclideanDistance(s: List[Double], o: List[Double]): Double = {
    //Sum all the distances
    var summ = 0.0
    for ((as, ao) <- (s zip o)) {
      summ += EuclideanDistance(as, ao)
    }
    return summ
  }

  def EuclideanDistance(s: Double, o: Double): Double = {
    //We expect both point to share the same x (time) value.
    //We can simplify the distance to an absolute difference.

    return math.abs(o - s)
  }
}
