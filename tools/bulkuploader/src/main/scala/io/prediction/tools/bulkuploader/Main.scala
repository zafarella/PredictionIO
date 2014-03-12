package io.prediction.tools.bulkuploader

/**
 * Run from root directory.
 *   % sbt "project toolsBulkUploader" run 
 *
 */


import io.prediction.commons.Config
import io.prediction.commons.appdata.Item
import com.github.nscala_time.time.Imports.DateTime

object BulkUploader {
  def main(args: Array[String]) {
    println("Bulkuploader!")
    val appid = 2

    val config = new Config()
    val items = config.getAppdataItems()

    def makeItem(iid: String, itypes: Seq[String]): Item =
      Item(
        id = iid,
        appid = appid,
        ct = DateTime.now,
        itypes = itypes,
        starttime = None,
        endtime = None,
        price = None,
        profit = None,
        latlng = None,
        inactive = None,
        attributes = None
      );

    items.insert(makeItem("item1", List("organic", "beef")))
    items.insert(makeItem("item2", List("beef")))
  }
}
