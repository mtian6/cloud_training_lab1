package tables;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class Stock_info {

    private static final String TABLE_NAME = "stock_info";

    public static String getTableName() {
        return TABLE_NAME;
    }

    /**
     * Stores result of the query in a Map.
     * @param rs
     * @param day_or_month Either "day" or "month" depending on the sql query given.
     * @param symbol_map
     * @param date
     * @return A mapping of symbol to date, total_volume, max_price, min_price, and closing_price.
     * @throws SQLException
     */
    public static Map storeResults(ResultSet rs, String day_or_month, Map symbol_map, String date) throws SQLException{
        rs.last();
        if (rs.getRow() == 0) {
            System.out.println("No data found");
        }
        rs.beforeFirst();

        while (rs.next()) {
            String sym = rs.getString("symbol");
            if (sym == null) {
                System.out.println("No data found (symbol is null)");
                break;
            }
            if (!symbol_map.containsKey(sym)){
                symbol_map.put(sym, new HashMap());
            }
            Map items_map = (Map)symbol_map.get(sym);
            if (hasColumn(rs, "max_price")) items_map.put("max_price", rs.getString("max_price"));
            if (hasColumn(rs, "min_price")) items_map.put("min_price", rs.getString("min_price"));
            if (hasColumn(rs, "total")) items_map.put("total_volume", rs.getString("total"));
            if (hasColumn(rs, "closing_price_day")) items_map.put("closing_price_day", rs.getString("closing_price_day"));
            if (hasColumn(rs, "closing_price_month")) items_map.put("closing_price_month", rs.getString("closing_price_month"));

            if (day_or_month.equals("day")) items_map.put("date", date);
            else items_map.put("date", date.substring(0,7));
        }
        return symbol_map;
    }

    /**
     * Displays result in console.
     * @param info_map
     */
    public static void displayResult(Map info_map) {
        for (Object key: info_map.keySet()) {
            Map symbol_map = (HashMap)info_map.get(key);
            for (Object key2: symbol_map.keySet())
                System.out.println(key2 + "-" + symbol_map.get(key2));
            System.out.println();
        }
    }

    /**
     * Checks if a specific column exists in the sql database table.
     * @param rs
     * @param column_name
     * @return
     */
    private static boolean hasColumn(ResultSet rs, String column_name) {
        try
        {
            rs.findColumn(column_name);
            return true;
        } catch (SQLException e)
        {
            //System.err.println("Column does not exist: " + e);
            return false;
        }
    }

    /**
     *
     * @param table_name
     * @param stocks
     * @return The query string ready to be executed.
     */
    public static String getInsertionQuery(String table_name, JSONArray stocks) {
        String query = "insert into " + table_name + " (volume, date, symbol, price) values ";

        JSONObject stock;

        //SimpleDateFormat timeformatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        //timeformatter.setTimeZone(TimeZone.getTimeZone("UTC-5"));

        //Note: removed timezone "+0000" from date, otherwise invalid date format
        for (int i = 0; i < stocks.size() - 1; i++) {
            stock = (JSONObject) stocks.get(i);
            query = query + "(" + stock.get("volume") + ", " + "\"" + stock.get("date").toString().split("\\+")[0] + "\"" +
                    ", " + "\"" + stock.get("symbol") + "\"" + ", " + stock.get("price") + "), ";
        }
        stock = (JSONObject) stocks.get(stocks.size() - 1);
        query = query + "(" + stock.get("volume") + ", " + "\"" + stock.get("date").toString().split("\\+")[0] + "\"" +
                ", " + "\"" + stock.get("symbol") + "\"" + ", " + stock.get("price") + ");";

        return query;
    }

    /**
     * Creates the sql query string that finds the maximum, minimum, and total volume of trades of each stock from either a given day or a month.
     * @param day_or_month Either "day" or "month".
     * @param date
     * @return
     */
    public static String getMaxMinAndVolumeForGivenDate(String day_or_month, String date) {
        String query = null;
        if (day_or_month.equals("day")) {
            query = "select symbol, max(price) as max_price, min(price) as min_price, sum(volume) as total from " + TABLE_NAME + " where date like '" + date + "%' group by symbol;";
        } else if (day_or_month.equals("month")) {
            query = "select symbol, max(price) as max_price, min(price) as min_price, sum(volume) as total from " + TABLE_NAME + " where date like '" + date.substring(0, 7) + "%' group by symbol;";
        }
        return query;
    }

    /**
     * Creates the sql query string that finds the closing price of each stock from either a given day or a month.
     * @param day_or_month Either "day" or "month".
     * @param date
     * @return
     */
    public static String getClosingPriceForGivenDate(String day_or_month, String date) {
        String query = null;
        if (day_or_month.equals("day")) {
            query = "select t1.symbol, t1.price as closing_price_day from " +
                    "(select symbol, price from " + TABLE_NAME + " where date = (select max(date) from " + TABLE_NAME +
                    " where date like '" + date + "%')) as t1 ;";
        } else if (day_or_month.equals("month")) {
            query = "select t2.symbol, t2.price as closing_price_month from " +
                    "(select symbol, price from " + TABLE_NAME + " where date = (select max(date) from " + TABLE_NAME +
                    " where date like '" + date.substring(0, 7) + "%') ) as t2;";
        }
        return query;
    }

    /**
     * Executes the sql queries and stores it in the return value as a Map.
     * @param stmt
     * @param date
     * @return
     * @throws SQLException
     */
    public static Map executeAndStoreQuery(Statement stmt, String date) throws SQLException{
        Map symbol_day_map = new HashMap();
        ResultSet rs1 = stmt.executeQuery(getMaxMinAndVolumeForGivenDate("day", date));
        symbol_day_map = storeResults(rs1, "day", symbol_day_map, date);
        ResultSet rs2 = stmt.executeQuery(getClosingPriceForGivenDate("day", date));
        symbol_day_map = storeResults(rs2, "day", symbol_day_map, date);

        Map symbol_month_map = new HashMap();
        ResultSet rs3 = stmt.executeQuery(getMaxMinAndVolumeForGivenDate("month", date));
        symbol_month_map = storeResults(rs3, "month", symbol_month_map, date);
        ResultSet rs4 = stmt.executeQuery(getClosingPriceForGivenDate("month", date));
        symbol_month_map = storeResults(rs4, "month", symbol_month_map, date);

        Map info_map = new HashMap();
        info_map.put("day", symbol_day_map);
        info_map.put("month", symbol_month_map);

        return info_map;
    }

}
