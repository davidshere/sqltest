package example

import scalikejdbc._

object Hello extends App {
  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton("jdbc:postgresql://0.0.0.0:5432/postgres", "postgres", "mysecretpassword")


  def runTestQuery: Int = {
    val result: Option[Int] = DB readOnly { implicit session =>
      sql"select 1 as v".map(r => r.int("v")).single.apply()
    }
    result.getOrElse(throw new RuntimeException("no results"))
  }

  println(runTestQuery)
}
