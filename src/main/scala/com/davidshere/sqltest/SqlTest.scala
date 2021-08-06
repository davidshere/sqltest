package com.davidshere.sqltest

import java.io.{File, InputStream}
import java.sql.{Connection, DriverManager, JDBCType, ResultSet, Types}
import java.util
import java.util.{ArrayList, HashMap, Map => JMap}

import com.davidshere.sqltest.SqlTest.{getClass, getExpected}
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvParser}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

import scala.collection.mutable.ListBuffer
import scala.io.Source
import collection.JavaConverters._
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.{Statement => ParserStatement}
import net.sf.jsqlparser.statement.select.{Join, PlainSelect, Select}
import net.sf.jsqlparser.util.TablesNamesFinder
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource, ClickHouseStatement}
import ru.yandex.clickhouse.domain.ClickHouseFormat
import ru.yandex.clickhouse.settings.ClickHouseQueryParam

trait DBInterface {
  def executeQuery(query: String): List[Map[String, String]]
  def loadCsvToDb(path: String, tableName: String): Unit
}

trait JDBCInterface extends DBInterface {
  val driverClassName: String
  def setupDriver(): Unit = Class.forName(driverClassName)

  def getConn: Connection

  def transformResult(result: ResultSet): List[Map[String, String]] = {

    val columnCount = result.getMetaData.getColumnCount
    val columnNames = (1 to columnCount).map(result.getMetaData.getColumnName).toList

    Iterator
      .continually(result.next)
      .takeWhile(identity)
      .map {_ => columnNames.map(n => n -> result.getString(n)).toMap}
      .toList
  }
}

class ClickHouseInterface extends JDBCInterface {
  val driverClassName = "ru.yandex.clickhouse.ClickHouseDriver"
  def getConn: ClickHouseConnection = {
    val url = "jdbc:clickhouse://172.17.0.1:8123"
    val dataSource = new ClickHouseDataSource(url)

    dataSource.getConnection
  }

  def executeQuery(query: String): List[Map[String, String]] =  {
    val conn: ClickHouseConnection = getConn
    val stmt: ClickHouseStatement = conn.createStatement()
    val params: JMap[ClickHouseQueryParam, String] = new HashMap()

    val res = stmt.executeQuery(query, params)
    conn.close()
    transformResult(res)
  }

  def loadCsvToDb(tableName: String, csv: InputStream): Unit = {
    val chi = new ClickHouseInterface
    val conn = chi.getConn
    val stmt = conn.createStatement()
    stmt
      .write()
      .table(tableName)
      .option("format_csv_delimiter", ",")
      .data(csv, ClickHouseFormat.CSVWithNames)
      .send()

    conn.close()
  }
}


class PGInterface extends JDBCInterface {
  override val driverClassName: String = "org.postgresql.Driver"
  def getConn: Connection = DriverManager.getConnection(s"jdbc:postgresql://0.0.0.0:5432/postgres")

  override def executeQuery(query: String): List[Map[String, String]] = {
    val conn = getConn
    val stmt = conn.createStatement()

    val res = stmt.executeQuery(query)
    transformResult(res)
  }

  override def loadCsvToDb(path: String, tableName: String): Unit = {}

}

class SparkInterface extends DBInterface {

  def loadCsvToDb(path: String, tableName: String): Unit = {
    val df = spark.read.option("header", "true").csv(path=path)
    df.createOrReplaceTempView(tableName)
  }

  override def executeQuery(query: String): List[Map[String, String]] = {
    val result = spark.sql(query)
    val stringResult = result.select(result.columns.map(c => col(c).cast(StringType)): _*)

    stringResult.collect.map(r => Map(stringResult.columns.zip(r.toSeq): _*)).toList
  }

  def tearDown(): Unit = {
    spark.close()
  }

  val spark = SparkSession
    .builder()
    .appName("unit test")
    .master("local[2]")
    .config("spark.broadcast.compress", "false")
    .config("spark.shuffle.compress", "false")
    .config("spark.shuffle.spill.compress", "false")
    .getOrCreate()

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

  def getSubQueriesInOrder(query: String) = {
    val queries: Seq[String] = Seq()
    val statement: ParserStatement = CCJSqlParserUtil.parse(query)
    val plainSelect = statement.asInstanceOf[Select].getSelectBody.asInstanceOf[PlainSelect]
    val joins: List[Join] = plainSelect.getJoins.asScala.toList
    val join: Join = joins(0)


  }
}


object SqlTest extends App {

  val createdTables = new ListBuffer[String]()
  /*
  def main(): Unit = {
    try {
      setUp()
      run()
    } catch {
      case e: Throwable => throw e

    }
    finally {
      tearDown()
    }
  }
  */
  def setUp(): Unit = {
    val chi = new ClickHouseInterface()
    val schema: String = Source.fromResource("table2.sql").mkString

    val createTableStatements = schema.strip.split(';')
    createTableStatements.foreach(stmt => {
      chi.executeQuery(stmt)
      createdTables += SQLParser.getTableNameFromCreateTable(stmt).head
    })

    val csv: InputStream = getClass.getClassLoader.getResourceAsStream("claims.csv")
    loadToDbFromCsv("default.claims", csv)
  }


  def run(): Unit = {
    val query: String = Source.fromResource("hcc.sql").mkString
    val chi = new ClickHouseInterface()
    val result = chi.executeQuery(query)

    val expectedFile = new File("/home/davidshere/src/sqltest/src/main/resources/expected.csv")
    val expected = getExpected(expectedFile)
    println(compare(result, expected))
  }

  def tearDown(): Unit = {
    val chi = new ClickHouseInterface()
    println(createdTables)
    createdTables.foreach(tbl => chi.executeQuery(s"DROP TABLE $tbl"))
  }




  private def getExpected(file: File):List[Map[String, String]] = {
    val mapper = new CsvMapper()
    mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY)
    val rows = mapper.readerFor(classOf[util.List[String]]).readValues[ArrayList[String]](file).readAll().asScala.map(_.asScala.toList).toList

    rows.tail.map(rows.head zip _).map(_.toMap).toList
  }



  private def compare(result: ResultSet, expected: List[Map[String, String]]): Boolean = transformResult(result).equals(expected)


  //main()
  // Inspired by https://github.com/tmalaska/SparkUnitTestingExamples
  /*
  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.broadcast.compress", "false")
  sparkConfig.set("spark.shuffle.compress", "false")
  sparkConfig.set("spark.shuffle.spill.compress", "false")
  sparkConfig.set("spark.io.compression.codec", "false")
  val sc = new SparkContext("local[2]", "unit test", sparkConfig)
  */
  def main(): Unit = {


  }
  main
}
