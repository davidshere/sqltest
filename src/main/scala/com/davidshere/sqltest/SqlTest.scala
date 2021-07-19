package com.davidshere.sqltest

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.{HashMap, Map}


import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.io.Source
import scala.jdk.CollectionConverters._
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.{Statement => ParserStatement}
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.util.TablesNamesFinder
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource, ClickHouseStatement}
import ru.yandex.clickhouse.settings.ClickHouseQueryParam

trait DBInterface {
  val driverClassName: String
  def setupDriver: Unit = Class.forName(driverClassName)

  def getConn: Connection

  def executeQuery(query: String): ResultSet

}

class ClickHouseInterface extends DBInterface {
  val driverClassName = "ru.yandex.clickhouse.ClickHouseDriver"
  def getConn = {
    val url = "jdbc:clickhouse://172.17.0.1:8123"
    val dataSource = new ClickHouseDataSource(url)

    dataSource.getConnection
  }

  def executeQuery(query: String): ResultSet =  {
    val conn: ClickHouseConnection = getConn
    val stmt: ClickHouseStatement = conn.createStatement()
    val params: Map[ClickHouseQueryParam, String] = new HashMap()

    val res = stmt.executeQuery(query, params)
    conn.close()
    res
  }
}

class PGInterface extends DBInterface {
  override val driverClassName: String = "org.postgresql.Driver"
  def getConn = DriverManager.getConnection(s"jdbc:postgresql://0.0.0.0:5432/postgres")

  override def executeQuery(query: String): ResultSet = {
    val conn = getConn
    val stmt = conn.createStatement()

    stmt.executeQuery(query)

    val testDBName = s"test_database_${Random.alphanumeric.take(6).mkString}"

    def createTestDB = {
      val sql = s"CREATE DATABASE ${testDBName}"
      stmt.executeUpdate(sql)
    }

    def destroyTestDB = {
      val sql = s"DROP DATABASE ${testDBName}"
      stmt.executeUpdate(sql)
    }
  }
}

object SQLParser {
  def getTableNameFromSimpleQuery(query: String): List[String] = {
    val statement: ParserStatement = CCJSqlParserUtil.parse(query)
    val selectStmt: Select = statement.asInstanceOf[Select]
    val nameFinder: TablesNamesFinder = new TablesNamesFinder()

    nameFinder.getTableList(selectStmt).asScala.toList
  }

  def getTableNameFromCreateTable(create: String): List[String] = {
    val statement: ParserStatement = CCJSqlParserUtil.parse(create)
    val createStmt: CreateTable = statement.asInstanceOf[CreateTable]
    val nameFinder: TablesNamesFinder = new TablesNamesFinder()

    nameFinder.getTableList(createStmt).asScala.toList
  }
}

object ResourceManager {

  def getTestSchema(filename: String): String = Source.fromResource(filename).mkString

  def getCsvData(filename: String): String = Source.fromResource(filename).mkString

}

object SqlTest extends App {

  val createdTables = new ListBuffer[String]()



  /*


 /*
  *
  */
  def setUp = {
    val schema: String = getTestSchema("table2.sql")
    val names = schema.strip.split(";").flatMap(getTableNameFromCreateTable(_))
    names.foreach(n => createdTables += n)
  }


  def tearDown = {
    // createdTables.foreach(tbl => stmt.executeUpdate(s"DROP TABLE ${tbl}")
    conn.close()
  }
  */

  def main = {
    // setUp
    val chi = new ClickHouseInterface()
    val ddl: String = ResourceManager.getTestSchema("table2.csv")
    val r = chi.executeQuery("create table t1 (v1 int) engine = TinyLog;")
    /*
    while (r.next()) {
      println(r.getInt(1), r.getString(2))
    }
    */

    r
    // tearDown
  }

  main
}
