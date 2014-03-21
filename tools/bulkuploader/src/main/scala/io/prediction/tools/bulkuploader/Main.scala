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
    val appid = 2package io.prediction.tools.bulkuploader

/**
 * Run from root directory.
 *   % sbt "project toolsBulkUploader" run
 *
 *   Create number of items, number of users, u2i action automatically. 
 *   To disable automatic creation set numUsers, numItems to 0
 */

import io.prediction.commons.Config
import io.prediction.commons.appdata.Item
import io.prediction.commons.appdata.User
import io.prediction.commons.appdata.U2IAction

import com.github.nscala_time.time.Imports.DateTime

object BulkUploader {
  def main(args: Array[String]) {
    println("Bulkuploader!")
    val appid = 1
    val numUsers = 20
    val numItems = 4
    makeItem(appid, numItems)
    makeUser(appid, numUsers)
  }

  def makeItem(appid: Int, numItems: Int = 0): Any = {
    val config = new Config()
    val items = config.getAppdataItems()
    println("makeItem!")
    def makeItem(iid: String, itypes: Seq[String] = List("a")): Item =
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
    if (numItems > 0) {
      for (a <- 1 to numItems) {
        println("makeItem!" + a)
        items.insert(makeItem("i" + a, List("a")))
      }
      return Some(0)
    }
    items.insert(makeItem("item1", List("organic", "beef")))
    items.insert(makeItem("item2", List("beef")))

  }

  def makeUser(appid: Int, numUsers: Int = 10) {
    val config = new Config()
    val users = config.getAppdataUsers()

    def makeUser(idd: String): User = User(
      id = idd,
      appid = appid,
      ct = DateTime.now,
      latlng = None,
      inactive = None,
      attributes = None);
    if (numItems > 0) {
      for (a <- 1 to numUsers) {
        //println( "Value of a: " + a );
        users.insert(makeUser("u" + a))
      }
      reutrn Some(0)
    }
    users.insert(makeUser("name1"))
    users.insert(makeUser("name2"))
  }

  def makeU2Iaction(appid: Int, numUsers: Int = 10) {
    val config = new Config()
    val users = config.getAppdataU2IActions()

    def makeU2IAction(iid: String, uid: String, action: String): U2IAction = U2IAction(
      appid = appid,
      action = action,
      uid = uid,
      iid = iid,
      t = DateTime.now,
      latlng = None,
      v = None,
      price = None);

    for (a <- 1 to numUsers) {
      println("Value of a: " + a);
      //users.insert(makeUser("u" + a))
    }

  }
}


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
