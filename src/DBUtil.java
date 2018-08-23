import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;


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

    public static void createAndUseDatabase(Statement stmt, String database_name) {
        String createDB = "create database if not exists " + database_name + ";";
        String useDB = "use " + database_name + ";";
        try {
            stmt.executeUpdate(createDB);
            stmt.executeUpdate(useDB);
        } catch (SQLException e) {
            processException(e);
        }
    }

    public static void createTable(Statement stmt, String table_name) {
        String drop_table = "drop table if exists " + table_name + ";";
        String create_table = "create table " + table_name + " (id INTEGER AUTO_INCREMENT, volume INTEGER, date DATETIME, symbol VARCHAR(20), price DECIMAL(19,2), PRIMARY KEY (id));";
        try {
            stmt.executeUpdate(drop_table);
            stmt.executeUpdate(create_table);
        } catch (SQLException e) {
            processException(e);
        }
    }

    public static void insertToTable(Statement stmt, String table_name, JSONArray stocks) throws ParseException{
        String query = "insert into " + table_name + " (volume, date, symbol, price) values ";
        JSONObject stock;

        if (stocks.size() <= 0) {
            System.out.println("Nothing to insert");
            return;
        }

        //SimpleDateFormat timeformatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        //timeformatter.setTimeZone(TimeZone.getTimeZone("UTC-5"));

        try {
            //Note: removed timezone "+0000" from date, otherwise invalid date format
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
