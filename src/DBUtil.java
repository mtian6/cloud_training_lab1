import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class DBUtil {
    private static final String USERNAME = "root";
    private static final String PASSWORD = "mysql123";
    private static final String CONN_STRING = "jdbc:mysql://localhost:3306?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&useSSL=false";

    public static Connection getConnection() throws SQLException {
         return DriverManager.getConnection(CONN_STRING, USERNAME, PASSWORD);
    }

    public static void processException(SQLException e) {
        System.err.println("Error message: " + e.getMessage());
        System.err.println("Error code: " + e.getErrorCode());
        System.err.println("SQL state: " + e.getSQLState());
    }

    public static void createDatabase(Statement stmt, String database_name) {
        String query = "create database if not exists " + database_name + ";";
        try {
            int rs = stmt.executeUpdate(query);
        } catch (SQLException e) {
            processException(e);
        }
    }

    public static void useDatabase(Statement stmt, String database_name) {
        String query = "use " + database_name + ";";
        try {
            int rs = stmt.executeUpdate(query);
        } catch (SQLException e) {
            processException(e);
        }
    }

    public static void createTable(Statement stmt, String table_name) {
        String query1 = "drop table if exists " + table_name + ";";
        String query2 = "create table " + table_name + " (id INTEGER AUTO_INCREMENT, volume INTEGER, date DATE, symbol VARCHAR(20), price DECIMAL(19,2), PRIMARY KEY (id));";
        try {
            int rs1 = stmt.executeUpdate(query1);
            int rs2 = stmt.executeUpdate(query2);
        } catch (SQLException e) {
            processException(e);
        }
    }

    public static void insertToTable(Statement stmt, String table_name, JSONArray stocks) {
        String query = "insert into " + table_name + " (volume, date, symbol, price) values ";
        JSONObject stock;

        if (stocks.size() <= 0) {
            System.out.println("Nothing to insert");
            return;
        }

        try {

            //Note: removed last "+0000" from date, otherwise invalid date format
            for (int i = 0; i < stocks.size() - 1; i++) {
                stock = (JSONObject) stocks.get(i);
                query = query + "(" + stock.get("volume") + ", " + "\"" + stock.get("date").toString().split("\\+")[0] + "\"" +
                        ", " + "\"" + stock.get("symbol") + "\"" + ", " + stock.get("price") + "), ";
            }
            stock = (JSONObject) stocks.get(stocks.size() - 1);
            query = query + "(" + stock.get("volume") + ", " + "\"" + stock.get("date").toString().split("\\+")[0] + "\"" +
                    ", " + "\"" + stock.get("symbol") + "\"" + ", " + stock.get("price") + ");";
            //System.out.println(query);
            int rs = stmt.executeUpdate(query);
            System.out.println("insertion complete: " + rs);
        } catch (SQLException e) {
            processException(e);
        }
    }
}
