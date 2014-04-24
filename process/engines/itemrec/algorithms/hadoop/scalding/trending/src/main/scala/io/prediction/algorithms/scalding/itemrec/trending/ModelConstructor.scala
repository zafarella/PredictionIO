package io.prediction.algorithms.scalding.itemrec.trending

import com.twitter.scalding._

import io.prediction.commons.scalding.appdata.Users
import io.prediction.commons.filepath.{ DataFile, AlgoFile }
import io.prediction.commons.scalding.modeldata.ItemRecScores

/**
 * Source:
 *   selectedItems.tsv
 *   itemRecScores.tsv
 * Sink:
 *   itemRecScores DB
 * Description:
 *   Read the itemRecScores.tsv and get additional attributes from selectedItems.tsv for each similiar items.
 *   Then write the result to model DB.
 *
 * Required args:
 * --dbType: <string> modeldata DB type (eg. mongodb) (see --dbHost, --dbPort)
 * --dbName: <string> (eg. predictionio_modeldata)
 *
 * --hdfsRoot: <string>. Root directory of the HDFS
 *
 * --appid: <int>
 * --engineid: <int>
 * --algoid: <int>
 * --modelSet: <boolean> (true/false). flag to indicate which set
 *
 * Optionsl args:
 * --dbHost: <string> (eg. "127.0.0.1")
 * --dbPort: <int> (eg. 27017)
 *
 * --evalid: <int>. Offline Evaluation if evalid is specified
 * --debug: <String>. "test" - for testing purpose
 *
 * Example:
 * batch:
 * scald.rb --hdfs-local io.prediction.algorithms.scalding.itemrec.trending.ModelConstructor --dbType mongodb --dbName modeldata --dbHost 127.0.0.1 --dbPort 27017 --hdfsRoot hdfs/predictionio/ --appid 34 --engineid 3 --algoid 9 --modelSet false
 *
 * offline eval
 * scald.rb --hdfs-local io.prediction.algorithms.scalding.itemrec.trending.ModelConstructor --dbType file --dbName modeldata_path/ --hdfsRoot hdfs/predictionio/ --appid 34 --engineid 3 --algoid 9 --modelSet false --evalid 15
 */
class ModelConstructor(args: Args) extends Job(args) {

  /**
   * parse args
   */
  val dbTypeArg = args("dbType")
  val dbNameArg = args("dbName")
  val dbHostArg = args.optional("dbHost")
  val dbPortArg = args.optional("dbPort") map (x => x.toInt)

  val hdfsRootArg = args("hdfsRoot")

  val appidArg = args("appid").toInt
  val engineidArg = args("engineid").toInt
  val algoidArg = args("algoid").toInt
  val evalidArg = args.optional("evalid") map (x => x.toInt)
  val OFFLINE_EVAL = (evalidArg != None) // offline eval mode

  val debugArg = args.list("debug")
  val DEBUG_TEST = debugArg.contains("test") // test mode

  val modelSetArg = args("modelSet").toBoolean
  val numRecommendationsArg = args("numRecommendations").toInt

  /**
   * input
   */
  val scores = Tsv(AlgoFile(hdfsRootArg, appidArg, engineidArg, algoidArg, evalidArg, "itemRecScores.tsv")).read
    .mapTo((0, 1) -> ('iid, 'score)) { fields: (String, Double) => fields }

  val items = Tsv(DataFile(hdfsRootArg, appidArg, engineidArg, algoidArg, evalidArg, "selectedItems.tsv")).read
    .mapTo((0, 1) -> ('iidx, 'itypes, 'itemKey)) { fields: (String, String) =>
      val (iidx, itypes) = fields // itypes are comma-separated String
      (iidx, itypes.split(",").toList, 1)
    }

  val users = Users(appId = appidArg, dbType = dbTypeArg, dbName = dbNameArg, dbHost = dbHostArg, dbPort = dbPortArg).readData('uid)
  val usersWithKey = users.map(() -> 'userKey) { u: Unit => 1 }

  /**
   * process & output
   */
  val p = scores.joinWithSmaller('iid -> 'iidx, items) // get items info for each iid
    // .groupAll { _.sortBy('score).reverse.toList[(String, Double, List[String])](('iid, 'score, 'itypes) -> 'iidsList) }
    .joinWithLarger('itemKey -> 'userKey, usersWithKey)
    .project('uid, 'iid, 'score, 'itypes)
    // .groupBy('uid) { _.sortedReverseTake[(Double, String)](('score, 'iid) -> 'pscore, numRecommendationsArg).toList[(String, Double, List[String])](('iid, 'score, 'itypes) -> 'iidsList) }
    .groupBy('uid) { _.sortBy('score).take(numRecommendationsArg) }
    .groupBy('uid) { _.toList[(String, Double, List[String])](('iid, 'score, 'itypes) -> 'iidsList) }

  val src = ItemRecScores(dbType = dbTypeArg, dbName = dbNameArg, dbHost = dbHostArg, dbPort = dbPortArg, algoid = algoidArg, modelset = modelSetArg)
  p.then(src.writeData('uid, 'iidsList, algoidArg, modelSetArg) _)

}
