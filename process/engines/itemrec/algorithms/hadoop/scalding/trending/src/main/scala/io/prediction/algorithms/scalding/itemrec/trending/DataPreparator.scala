package io.prediction.algorithms.scalding.itemrec.trending

import com.github.nscala_time.time.Imports._
import com.twitter.scalding._

import io.prediction.commons.scalding.appdata.{ Items, U2iActions }
import io.prediction.commons.filepath._

/**
 * Source: appdata DB (items, u2iActions)
 * Sink: ratings.tsv, selectedItems.tsv
 * Descripton:
 *   Prepare data for itemrec.trending algo. Read from appdata DB and store selected items
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
 * --windowSize: <string>
 * --action: <string>
 *
 * Optional args:
 * --dbHost: <string> (eg. "127.0.0.1")
 * --dbPort: <int> (eg. 27017)
 * --endTime: <long> (by default now)
 * --numWindows: <int>
 * --windowSizeExplicit: <int>
 *
 * --itypes: <string separated by white space>. eg "--itypes type1 type2". If no --itypes specified, then ALL itypes will be used.
 * --evalid: <int>. Offline Evaluation if evalid is specified
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

  val now = DateTime.now.millis
  val windowSizeArg = args.getOrElse("windowSize", "nosize") // the only time this shouldn't be supplied is during testing
  val actionArg = args("action")
  val endTimeArg = args.getOrElse("endTime", now.toString).toLong // input values should only be used for testing
  val numWindowsArg = args.getOrElse("numWindows", "-1").toInt // should only be used for testing

  // the number of seconds in each of the following
  val windowSize = windowSizeArg match {
    case "hour" => 3600
    case "day" => 86400
    case "week" => 604800
    case _ => args.getOrElse("windowSizeExplicit", "-1").toInt
  }
  var numWindows = numWindowsArg
  if (numWindows <= 0) {
    numWindows = windowSizeArg match {
      case "hour" => 24 * 3
      case "day" => 7 * 4
      case "week" => 4 * 5
      case _ => -1
    }
  }

  val startTime = endTimeArg - windowSize * numWindows

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
  val timeseriesSink = Tsv(DataFile(hdfsRootArg, appidArg, engineidArg, algoidArg, evalidArg, "ratings.tsv"))
  val selectedItemsSink = Tsv(DataFile(hdfsRootArg, appidArg, engineidArg, algoidArg, evalidArg, "selectedItems.tsv"))

  /**
   * computation
   */
  u2i.joinWithSmaller('iid -> 'iidx, items)
    .filter('action, 't) { fields: (String, String) =>
      val (action, t) = fields
      action == actionArg && t.toLong >= startTime && t.toLong < endTimeArg
    }
    .groupBy('iid) {
      _.foldLeft('t -> 'timeseries)(Array.fill[Int](numWindows)(0)) {
        (seriesSoFar: Array[Int], t: String) =>
          seriesSoFar(((t.toLong - startTime) / windowSize).toInt) += 1
          seriesSoFar
      }
    }
    .map('timeseries -> 'timeseriesstring) {
      timeseries: Array[Int] =>
        timeseries.mkString(",")
    }
    .project('iid, 'timeseriesstring)
    .write(timeseriesSink)

  items
    .mapTo(('iidx, 'itypes) -> ('iidx, 'itypes)) { fields: (String, List[String]) =>
      val (iidx, itypes) = fields
      (iidx, itypes.mkString(","))
    }
    .write(selectedItemsSink)

}
