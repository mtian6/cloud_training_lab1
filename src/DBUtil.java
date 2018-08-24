import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.sql.*;


public class DBUtil {
    private static final String USERNAME = "root";
    private static final String PASSWORD = "mysql123";
    private static final String CONN_STRING = "jdbc:mysql://localhost:3306?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&useSSL=false";


    /**
     *
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
         return DriverManager.getConnection(CONN_STRING, USERNAME, PASSWORD);
    }

    /**
     * Customized exception messages for SQLException.
     * @param e
     */
    public static void processException(SQLException e) {
        System.err.println("Error message: " + e.getMessage());
        System.err.println("Error code: " + e.getErrorCode());
        System.err.println("SQL state: " + e.getSQLState());
    }

    /**
     * Creates a database if it does not exist and switches to that database.
     * @param stmt
     * @param database_name
     */
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

    /**
     * Creates a new table (drops old table if already exists).
     * @param stmt
     * @param table_name
     */
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

    /**
     * Inserts data into a table.
     * @param stmt
     * @param query
     */
    public static void insertToTable(Statement stmt, String query) {
        try {
            //System.out.println(query);
            int rs = stmt.executeUpdate(query);
            System.out.println("insertion complete: " + rs + " rows of data\n");
        } catch (SQLException e) {
            processException(e);
        }
    }
}
