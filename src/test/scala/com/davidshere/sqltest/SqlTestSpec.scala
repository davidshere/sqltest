package com.davidshere.sqltest

import org.scalatest.funsuite._

class SqlTestSpec extends AnyFunSuite {
  test("Parser should parse table names from SELECT") {
    val queryBasic = "SELECT * FROM table1"
    assert(SQLParser.getTableNameFromSimpleQuery(queryBasic) == List("table1"))

    val queryWhere = "SELECT * FROM table1 WHERE starfish='green'"
    assert(SQLParser.getTableNameFromSimpleQuery(queryWhere) == List("table1"))

    val queryJoin = "SELECT * FROM table1 JOIN table2 USING(starfish)"
    assert(SQLParser.getTableNameFromSimpleQuery(queryJoin) == List("table1", "table2"))

    val queryAlias = "SELECT * FROM table1 JOIN (select * from table2) as fish using (starfish)"
    assert(SQLParser.getTableNameFromSimpleQuery(queryAlias) == List("table1", "table2"))

    val queryWith = "WITH tmp AS (SELECT * FROM table1) SELECT * from tmp;"
    assert(SQLParser.getTableNameFromSimpleQuery(queryWith) == List("table1"))
  }

  test("Parse should parse table names from CREATE") {
    val stmt = "CREATE TABLE table1 (val1 INT PRIMARY KEY)"
    assert(SQLParser.getTableNameFromCreateTable(stmt) == List("table1"))

  }
}
