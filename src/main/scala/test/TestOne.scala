package test

import java.sql.{Connection, ResultSet, Statement}

import com.util.DBConnectionPool


object TestOne {
    def main(args: Array[String]): Unit = {
        val connection: Connection = DBConnectionPool.getConn()

        val statement: Statement = connection.createStatement()
        val rs : ResultSet = statement.executeQuery("select * from t1")

        while (rs.next()) {
            println(rs.getInt(1))
        }
        DBConnectionPool.releaseCon(connection)

    }

}
