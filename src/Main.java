import com.mysql.cj.xdevapi.JsonParser;

import java.io.*;
import java.net.URL;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import tables.Stock_info;


public class Main {

    private static final String DATABASE_NAME = "stocks";
    private static final String URL = "https://bootcamp-training-files.cfapps.io/week1/week1-stocks.json";

    private static Stock_info table = new Stock_info();

    private static final String DATE = "2018-06-22";


    public static void main(String[] args) throws Exception{

        JSONArray stocks = getData(URL);
//        System.out.println(stocks.get(0));  //each one is of type JSONObject
//        System.out.println(stocks.get(1));
//
//        System.out.println(stocks.get(0).getClass().getSimpleName()); //prints JSONObject

        //JSONObject stock = (JSONObject)stocks.get(0);
        //System.out.println(stock.get("volume"));

        Statement stmt = null;
        Connection conn = null;
        String table_name = table.getTableName();

        try {
            conn = DBUtil.getConnection();
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            DBUtil.createDatabase(stmt, DATABASE_NAME);
            DBUtil.useDatabase(stmt, DATABASE_NAME);
            DBUtil.createTable(stmt, table_name);

        } catch (SQLException e) {
            DBUtil.processException(e);
        } catch (Exception e) {
            System.out.println(e);
        }


        DBUtil.insertToTable(stmt, table_name, stocks);

        Map symbol_map = storeQueryResults(stmt);
        for (Object key: symbol_map.keySet()) System.out.println(key + "-" + symbol_map.get(key)); // print stored key-value map

        //processUserCommands(stmt);

        if (conn != null){
            conn.close();
        }
    }

    public static JSONArray getData(String url) throws Exception {
        URL oracle = new URL(url);
        BufferedReader jsonString = new BufferedReader(new InputStreamReader(oracle.openStream()));

        JSONParser parser = new JSONParser();
        Object obj = parser.parse(jsonString);
        JSONArray stocks = (JSONArray)obj;
        return stocks;
    }

    public static Map storeQueryResults(Statement stmt) throws SQLException {
        Map symbol_map = new HashMap();

        String q1 = table.getHighestPriceForGivenDate("all", DATE);
        ResultSet rs1 = stmt.executeQuery(q1);
        symbol_map = table.storeResults(rs1, "max", symbol_map, DATE);

        String q2 = table.getLowestPriceForGivenDate("all", DATE);
        ResultSet rs2 = stmt.executeQuery(q2);
        symbol_map = table.storeResults(rs2, "min", symbol_map, DATE);

        String q3 = table.getTotalVolumeTradedForGivenDate("all", DATE);
        ResultSet rs3 = stmt.executeQuery(q3);
        symbol_map = table.storeResults(rs3, "total", symbol_map, DATE);

        String q4 = table.getClosingPriceForGivenDate("all", DATE);
        ResultSet rs4 = stmt.executeQuery(q4);
        symbol_map = table.storeResults(rs4, "closing", symbol_map, DATE);

        return symbol_map;
    }

    public static void processUserCommands(Statement stmt) {
        try {
            String method = InputHelper.getInput("For a given date, do you want to find\n" +
                    "(max) max stock price\n" +
                    "(min) min stock price\n" +
                    "(total) total number of trades\n" +
                    "(closing) closing price?\n");
            String name = InputHelper.getInput("Enter stock symbol (or 'all'): ");
            String date = InputHelper.getInput("Enter a date (yyyy-mm-dd): ");

            String q = null;
            if (method.equals("max")){
                q = table.getHighestPriceForGivenDate(name, date);
            } else if (method.equals("min")) {
                q = table.getLowestPriceForGivenDate(name, date);
            } else if (method.equals("total")) {
                q = table.getTotalVolumeTradedForGivenDate(name, date);
            } else if (method.equals("closing")) {
                q = table.getClosingPriceForGivenDate(name, date);
            }
            else {
                System.out.println("Invalid input");
            }

            if (q != null) {
                ResultSet result = stmt.executeQuery(q);
                table.displayData(result);
            }

        } catch (Exception e) {
            System.err.println("Invalid input: " + e);
        }
    }

}
