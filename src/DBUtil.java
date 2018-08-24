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
