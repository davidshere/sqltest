package example

import scala.io.Source
import scala.util.Random

import java.sql.{
  Connection,
  DriverManager,
  ResultSet,
  Statement
}

object Hello extends App {

  Class.forName("org.postgresql.Driver")
  def getDBConn(dbName: String): Connection = DriverManager.getConnection(
    s"jdbc:postgresql://0.0.0.0:5432/${dbName}",
    "postgres",
    "mysecretpassword"
  )

  val conn: Connection = getDBConn("postgres")
  val stmt: Statement = conn.createStatement()

  val testDBName = s"test_database_${Random.alphanumeric.take(6).mkString}"
  val createdTables: Seq[String] = Seq() 

  def runTestQuery: Int = {
    val query: String = "select 1 as v"
    val result: ResultSet = stmt.executeQuery(query)
    result.next()
    result.getInt("v")
  }

  def getTestSchema(filename: String): String =
    Source.fromResource(filename).mkString

  def getCsvData(filename: String): String = Source.fromResource(filename).mkString


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

  def main = {
    setUp
    println(runTestQuery)
    val schema = getTestSchema("table2.sql")
    schema.split(';').foreach(stmt.executeUpdate(_))
    tearDown
  }

  main
}
