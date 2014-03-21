package io.prediction.tools.bulkuploader

/**
 * Run from root directory.
 *   % sbt "project toolsBulkUploader" run
 *
 *   Automatic creation:
 *   Create number of items, number of users, u2i action automatically.
 *   To disable automatic creation set numUsers, numItems to 0
 */

import io.prediction.commons.Config
import io.prediction.commons.appdata.Item
import io.prediction.commons.appdata.User
import io.prediction.commons.appdata.U2IAction
import scala.util.Random
import scala.language.postfixOps

import com.github.nscala_time.time.Imports.DateTime

object BulkUploader {
  def main(args: Array[String]) {
    println("Bulkuploader!")
    val appid = 1
    val numUsers = 20
    val numItems = 90
    makeItem(appid, numItems)
    makeUser(appid, numUsers)
    makeU2Iaction(appid, numUsers, numItems)
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

  def makeUser(appid: Int, numUsers: Int = 10): Any = {
    val config = new Config()
    val users = config.getAppdataUsers()

    def makeUser(idd: String): User = User(
      id = idd,
      appid = appid,
      ct = DateTime.now,
      latlng = None,
      inactive = None,
      attributes = None);
    if (numUsers > 0) {
      for (a <- 1 to numUsers) {
        //println( "Value of a: " + a );

        //val r = range(rnd.nextInt(range length))
        //println("Value of a: " + randomInt);
        users.insert(makeUser("u" + a))
      }
      return Some(0)
    }
    users.insert(makeUser("name1"))
    users.insert(makeUser("name2"))
  }

  /**
   *  creation of u2i. creates for each user 3 u2i randomly. 
   *  two are view, and one conversion. 
   * 
   * /
  def makeU2Iaction(appid: Int, numUsers: Int = 0, numItems: Int = 0) {
    val config = new Config()
    val u2i = config.getAppdataU2IActions()

    def makeU2IAction(uid: String, iid: String, action: String): U2IAction = U2IAction(
      appid = appid,
      action = action,
      uid = uid,
      iid = iid,
      t = DateTime.now,
      latlng = None,
      v = None,
      price = None);

    for (a <- 1 to numUsers) {
      val rnd = new scala.util.Random
      val range = 1 to numItems
      var i = range(rnd.nextInt(range length))
      u2i.insert(makeU2IAction("u" + a, "i" + i, "view"))
      i = range(rnd.nextInt(range length))
      u2i.insert(makeU2IAction("u" + a, "i" + i, "view"))
      i = range(rnd.nextInt(range length))
      u2i.insert(makeU2IAction("u" + a, "i" + i, "conversion"))
    }

  }
}
