package io.prediction.algorithms.scalding.itemtrend.generic

import com.twitter.scalding._

import io.prediction.commons.scalding.appdata.{ Items, U2iActions }
import io.prediction.commons.filepath._

/**
 * Source: appdata DB (items, u2iActions)
 * Sink: selectedItems.tsv, ratings.tsv
 * Descripton:
 *   Generic data preparator. Read from appdata DB and store selected items
 *   and ratings into a file.
 *   (appdata store -> DataPreparator -> HDFS)
 *
 * Required args:
 * --dbType: <string> (eg. mongodb) (see --dbHost, --dbPort)
 * --dbName: <string> appdata database name. (eg predictionio_appdata, or predictionio_training_appdata)
 *
 * --hdfsRoot: <string>. Root directory of the HDFS
 *
 * --appid: <int>
 * --engineid: <int>
 * --algoid: <int>
 *
 * --viewParam: <string>. (number 1 to 5, or "ignore")
 * --likeParam: <string>
 * --dislikeParam: <string>
 * --conversionParam: <string>
 *
 * Optional args:
 * --dbHost: <string> (eg. "127.0.0.1")
 * --dbPort: <int> (eg. 27017)
 *
 * --itypes: <string separated by white space>. eg "--itypes type1 type2". If no --itypes specified, then ALL itypes will be used.
 * --evalid: <int>. Offline Evaluation if evalid is specified
 * --debug: <String>. "test" - for testing purpose
 *
 * Example:
 * Batch:
 * scald.rb --hdfs-local io.prediction.algorithms.scalding.itemrec.generic.DataPreparator --dbType mongodb --dbName appdata --dbHost 127.0.0.1 --dbPort 27017 --hdfsRoot hdfs/predictionio/ --appid 34 --engineid 3 --algoid 9 --itypes t2 --viewParam 2 --likeParam 5 --dislikeParam 1 --conversionParam 4 --conflictParam latest
 *
 * Offline Eval:
 * scald.rb --hdfs-local io.prediction.algorithms.scalding.itemrec.generic.DataPreparator --dbType mongodb --dbName training_appdata --dbHost 127.0.0.1 --dbPort 27017 --hdfsRoot hdfs/predictionio/ --appid 34 --engineid 3 --algoid 9 --itypes t2 --viewParam 2 --likeParam 5 --dislikeParam 1 --conversionParam 4 --conflictParam latest --evalid 15
 *
 */
class DataPreparator(args: Args) extends Job(args) {

  /**
   * parse arguments
   */
  val dbTypeArg = args("dbType")
  val dbNameArg = args("dbName")
  val dbHostArg = args.optional("dbHost")
  val dbPortArg = args.optional("dbPort") map (x => x.toInt) // becomes Option[Int]

  val hdfsRootArg = args("hdfsRoot")

  val appidArg = args("appid").toInt
  val engineidArg = args("engineid").toInt
  val algoidArg = args("algoid").toInt
  val evalidArg = args.optional("evalid") map (x => x.toInt)
  val OFFLINE_EVAL = (evalidArg != None) // offline eval mode

  val preItypesArg = args.list("itypes")
  val itypesArg: Option[List[String]] = if (preItypesArg.mkString(",").length == 0) None else Option(preItypesArg)

  // Default argument values
  val defaultStart = System.currentTimeMillis / 1000
  val defaultWindowSize = "hour"
  val defaultNumWindows = 20
  val defaultAction = "view"

  val startTimeArg = args.getOrElse("startTime", defaultStart.toString).toLong
  val windowSizeArg = args.getOrElse("windowSize", defaultWindowSize.toString)
  val numWindowsArg = args.getOrElse("numWindows", defaultNumWindows.toString).toInt
  val actionArg = args.getOrElse("action", defaultAction.toString)

  // the number of seconds in each of the following
  val windowSize = windowSizeArg match {
    case "hour" => 3600
    case "day" => 86400
    case "week" => 604800
    case "year" => 31536000
    case _ => -1 
  }
  val endTime = startTimeArg - windowSize*numWindowsArg

  val debugArg = args.list("debug")
  val DEBUG_TEST = debugArg.contains("test") // test mode

  /**
   * constants
   */
  final val ACTION_RATE = "rate"
  final val ACTION_LIKE = "like"
  final val ACTION_DISLIKE = "dislike"
  final val ACTION_VIEW = "view"
  final val ACTION_VIEWDETAILS = "viewDetails"
  final val ACTION_CONVERSION = "conversion"

  /**
   * source
   */
  // get appdata
  // NOTE: if OFFLINE_EVAL, read from training set, and use evalid as appid when read Items and U2iActions
  val trainingAppid = if (OFFLINE_EVAL) evalidArg.get else appidArg

  // get items data
  val items = Items(appId = trainingAppid, itypes = itypesArg,
    dbType = dbTypeArg, dbName = dbNameArg, dbHost = dbHostArg, dbPort = dbPortArg).readData('iidx, 'itypes)

  val u2i = U2iActions(appId = trainingAppid,
    dbType = dbTypeArg, dbName = dbNameArg, dbHost = dbHostArg, dbPort = dbPortArg).readData('action, 'uid, 'iid, 't, 'v)

  /**
   * sink
   */
  val sink = Tsv(DataFile(hdfsRootArg, appidArg, engineidArg, algoidArg, evalidArg, "timeseries.tsv"))

  /**
   * computation
   */
  u2i.joinWithSmaller('iid -> 'iidx, items)
    .filter('action, 't) { fields: (String, String) =>
      val (action, t) = fields
      action == actionArg && t.toLong >= endTime && t.toLong <= startTimeArg
    }.groupBy('iid) { 
      _.foldLeft('t -> 'timeseries)(Array.fill[Int](numWindowsArg)(0)) {
        (seriesSoFar: Array[Int], time: String) =>
          seriesSoFar(((time.toLong-endTime)/windowSize).toInt) += 1
          seriesSoFar
      }
    }.map('timeseries ->'timeseriesstring) {
      timeseries:Array[Int] =>
        timeseries.mkString(",")
    }.project('iid, 'timeseriesstring)
    .write(sink)

}
