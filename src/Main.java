import com.mysql.cj.xdevapi.JsonParser;

import java.io.*;
import java.net.URL;
import java.sql.*;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import tables.Stock_info;


public class Main {

    private static final String DATABASE_NAME = "stocks";
    private static final String URL = "https://bootcamp-training-files.cfapps.io/week1/week1-stocks.json";

    private static Stock_info table = new Stock_info();

    private static String date = null;


    public static void main(String[] args) throws Exception{

        JSONArray stocks = getData(URL);

        Statement stmt = null;
        Connection conn = null;
        String table_name = table.getTableName();

        try {
            conn = DBUtil.getConnection();
            System.out.println("connection established...");

            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            DBUtil.createAndUseDatabase(stmt, DATABASE_NAME);
            DBUtil.createTable(stmt, table_name);

        } catch (SQLException e) {
            DBUtil.processException(e);
        } catch (Exception e) {
            System.out.println(e);
        }

        String query = table.getInsertionQuery(table_name, stocks);
        DBUtil.insertToTable(stmt, query);

        //Get user input for date
        date = InputHelper.getDateInput("Enter a date (yyyy-MM-dd): ");
        if (date == null) {
            return;
        }
        System.out.println();

        Map info_map = table.executeAndStoreQuery(stmt, date);

        //display result
        table.displayResult(info_map);

        if (conn != null){
            conn.close();
            System.out.println("connection closed");
        }
    }

    /**
     * Gets JSON data from a given URL and parses it into a JSONArray.
     * @param url
     * @return
     * @throws Exception
     */
    public static JSONArray getData(String url) throws Exception {
        URL oracle = new URL(url);
        BufferedReader jsonString = new BufferedReader(new InputStreamReader(oracle.openStream()));

        JSONParser parser = new JSONParser();
        Object obj = parser.parse(jsonString);
        JSONArray stocks = (JSONArray)obj;
        return stocks;
    }


}
