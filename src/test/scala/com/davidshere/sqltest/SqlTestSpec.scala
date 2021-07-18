package com.davidshere.sqltest

import org.scalatest.funsuite._

class SqlTestSpec extends AnyFunSuite {
  test("Parser should parse table names from SELECT") {
    val queryBasic = "SELECT * FROM table1"
    assert(SqlTest.getTableNameFromSimpleQuery(queryBasic) == List("table1"))

    val queryWhere = "SELECT * FROM table1 WHERE starfish='green'"
    assert(SqlTest.getTableNameFromSimpleQuery(queryWhere) == List("table1"))

    val queryJoin = "SELECT * FROM table1 JOIN table2 USING(starfish)"
    assert(SqlTest.getTableNameFromSimpleQuery(queryJoin) == List("table1", "table2"))

    val queryAlias = "SELECT * FROM table1 JOIN (select * from table2) as fish using (starfish)"
    assert(SqlTest.getTableNameFromSimpleQuery(queryAlias) == List("table1", "table2"))

    val queryWith = "WITH tmp AS (SELECT * FROM table1) SELECT * from tmp;"
    assert(SqlTest.getTableNameFromSimpleQuery(queryWith) == List("table1"))
  }

  test("Parse should parse table names from CREATE") {
    val stmt = "CREATE TABLE table1 (val1 INT PRIMARY KEY)"
    assert(SqlTest.getTableNameFromCreateTable(stmt) == List("table1"))

  }
}
