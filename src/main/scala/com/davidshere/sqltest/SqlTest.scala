package com.davidshere.sqltest

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import scala.io.Source
import scala.util.Random
import scala.jdk.CollectionConverters._
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.{Statement => ParserStatement}
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.util.TablesNamesFinder

object SqlTest extends App {
  /*
  Class.forName("org.postgresql.Driver")
  def getDBConn(dbName: String): Connection = DriverManager.getConnection(
    s"jdbc:postgresql://0.0.0.0:5432/${dbName}",
    "postgres",
    "mysecretpassword"
  )


  val conn: Connection = getDBConn("postgres")
  val stmt: Statement = conn.createStatement()
  */
  val testDBName = s"test_database_${Random.alphanumeric.take(6).mkString}"
  val createdTables: Seq[String] = Seq()
  /*
  def runTestQuery: Int = {
    val query: String = "select 1 as v"
    val result: ResultSet = stmt.executeQuery(query)
    result.next()
    result.getInt("v")
  }
  */
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

  def getTestSchema(filename: String): String =
    Source.fromResource(filename).mkString

  def getCsvData(filename: String): String = Source.fromResource(filename).mkString

  /*
  def createTestDB = {
    val sql = s"CREATE DATABASE ${testDBName}"
    stmt.executeUpdate(sql)
  }

  def destroyTestDB = {
    val sql = s"DROP DATABASE ${testDBName}"
    stmt.executeUpdate(sql)
  }

 /*
  *
  */
  def setUp = {
    createTestDB
  }

  def tearDown = {
    destroyTestDB
    conn.close()
  }
  */

  def main = {
    // setUp
    val schema: String = getTestSchema("table2.sql")
    val creates = schema.strip.split(";").map(_.strip)
    val names = creates.map(getTableNameFromCreateTable)
    println(names.length)
    println(names.foreach(_.mkString))
    println(createdTables)
    // schema.split(';').foreach(stmt.executeUpdate(_))
    // tearDown
  }

  main
}
