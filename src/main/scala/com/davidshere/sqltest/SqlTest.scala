package com.davidshere.sqltest

import java.io.File
import java.sql.{Connection, DriverManager, JDBCType, ResultSet, Types}
import java.util
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
import ru.yandex.clickhouse.domain.ClickHouseFormat
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
  }

  val testDBName = s"test_database_${Random.alphanumeric.take(6).mkString}"

  def createTestDB = {
    val sql = s"CREATE DATABASE ${testDBName}"
    val conn = getConn
    val stmt = conn.createStatement()
    stmt.executeUpdate(sql)
    conn.close()
  }

  def destroyTestDB = {
    val sql = s"DROP DATABASE ${testDBName}"
    val conn = getConn
    val stmt = conn.createStatement()
    stmt.executeUpdate(sql)
    conn.close()
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


object SqlTest extends App {

  val createdTables = new ListBuffer[String]()

  def setUp = {
    val chi = new ClickHouseInterface()
    val schema: String = Source.fromResource("table2.sql").mkString

    val createTableStatments = schema.strip.split(';')

    val names = createTableStatments.flatMap(SQLParser.getTableNameFromCreateTable(_))

    createTableStatments.foreach(stmt => {
      chi.executeQuery(stmt)
      createdTables += SQLParser.getTableNameFromCreateTable(stmt).head
    })


    val conn = chi.getConn
    val stmt = conn.createStatement()
    stmt
      .write()
      .table("default.claims")
      .option("format_csv_delimiter", ",")
      .data(new File("/home/davidshere/src/sqltest/src/main/resources/claims.csv"), ClickHouseFormat.CSVWithNames)
      .send()

    conn.close()
  }

  def run = {
    val query: String = Source.fromResource("hcc.sql").mkString
    val chi = new ClickHouseInterface()
    val result = chi.executeQuery(query)
    val transformedResult = transformResult(result)

  }

  private def rowToMap(result: ResultSet, columnNames: List[String], columnTypes: List[JDBCType]): List[(String, AnyRef)] = {
    return columnNames.map(n => n -> result.getObject(n))
    /*
    (columnNames zip columnTypes).map({
        case (n, JDBCType.INTEGER) => n -> result.getInt(n)
        case (n, JDBCType.BIGINT) => n -> result.getInt(n)
        case (n, JDBCType.VARCHAR) => n -> result.getString(n)
        case (n, _) => n -> result.getObject(n)
      })

     */
    }

  private def transformResult(result: ResultSet) = { //: List[Map[String, AnyVal]] =

    val columnCount = result.getMetaData.getColumnCount
    val columnNames = (1 to columnCount).map(result.getMetaData.getColumnName).toList
    val columnTypes = (1 to columnCount).map(result.getMetaData.getColumnType).map(JDBCType.valueOf(_)).toList

    val i = Iterator
      .continually(result.next)
      .takeWhile(identity)
      .map {_ => rowToMap(result, columnNames, columnTypes).toMap}
        .toList

    println(i)
    println(columnNames zip columnTypes)
    /*
 }
    })
    while (result.next() != null) {
      val m: Map[String, AnyVal] = new HashMap[String, AnyVal]()
      columnNames.zip(columnTypes) match {
        case e: (_, JDBCType) =>
      }
    }
    */
//    println(result)
  }

  def compare(result: ResultSet, expected: List[Map[String, AnyVal]]) = {

  }


  def tearDown = {
    val chi = new ClickHouseInterface()
    createdTables.foreach(tbl => chi.executeQuery(s"DROP TABLE ${tbl}"))
  }

  def main = {
    try {
      setUp
      println(createdTables)
      run
    } catch {
      case e: Throwable => throw e

    }
    finally {
      tearDown
    }

  }

  main
}
