package example

import scala.io.Source
import scala.util.Random

import scalikejdbc._

import ch.qos.logback.classic.{Level,Logger}
import org.slf4j.LoggerFactory

object Hello extends App {
  // Set logging level
  LoggerFactory
    .getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    .asInstanceOf[Logger]
    .setLevel(Level.INFO)

  // Connect to DB
  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton("jdbc:postgresql://0.0.0.0:5432/postgres", "postgres", "mysecretpassword")

  val testDBName = s"test_database_${Random.alphanumeric.take(6).mkString}"
  val createdTables: Seq[String] = Seq() 

  def runTestQuery: Int = {
    val result: Option[Int] = DB readOnly { implicit session =>
      sql"select 1 as v".map(r => r.int("v")).single.apply()
    }
    result.getOrElse(throw new RuntimeException("no results"))
  }

  def createTestDB = DB autoCommit { implicit session =>
    val dbName = SQLSyntax.createUnsafely(testDBName)
    sql"CREATE DATABASE ${dbName}".execute.apply()
  }

  def destroyTestDB = DB autoCommit { implicit session =>
    val dbName = SQLSyntax.createUnsafely(testDBName)
    println(dbName)
    sql"DROP DATABASE ${dbName}".execute.apply()
  }

 /*
  *
  */
  def setUp = {
    createTestDB
  }

  def tearDown = {
    destroyTestDB
  }

  def main = {
    setUp
    println(runTestQuery)
    tearDown
  }

  main
}
