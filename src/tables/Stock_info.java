package tables;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

public class Stock_info {

    private static final String TABLE_NAME = "stock_info";

    public static String getTableName() {
        return TABLE_NAME;
    }

    public static Map storeResults(ResultSet rs, String method, Map symbol_map, String date) throws SQLException{
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
            if (method == "maxmin") {
                items_map.put("max_price", rs.getString("max_price"));
                items_map.put("min_price", rs.getString("min_price"));
                items_map.put("total_volume", rs.getString("total"));
            } else if (method == "closing") {
                items_map.put("closing_price_day", rs.getString("closing_price_day"));
                items_map.put("closing_price_month", rs.getString("closing_price_month"));
            }
            items_map.put("date", date);
        }
        return symbol_map;
    }

    public static void displayData(ResultSet rs) throws SQLException {
        rs.last();
        if (rs.getRow() == 0) {
            System.out.println("No data found");
        }

        rs.beforeFirst();
        while (rs.next()) {
            StringBuffer buffer = new StringBuffer();

            String sym = rs.getString("symbol");
            if (sym == null) {
                System.out.println("No data found (symbol is null)");
                break;
            }
            buffer.append(sym);

            NumberFormat nf = NumberFormat.getCurrencyInstance();
            if (hasColumn(rs, "max_price")) {
                String formattedPrice = nf.format(rs.getDouble("max_price"));
                buffer.append(" (max " + formattedPrice + ")");
            }
            if (hasColumn(rs, "min_price")) {
                String formattedPrice = nf.format(rs.getDouble("min_price"));
                buffer.append(" (min " + formattedPrice + ")");
            }
            if (hasColumn(rs, "total")) {
                Double totalVolume = rs.getDouble("total");
                buffer.append(" (total volume " + totalVolume + ")");
            }
            if (hasColumn(rs, "closing_price_month")) {
                String formattedPrice = nf.format(rs.getDouble("closing_price_month"));
                buffer.append(" (month " + formattedPrice + ")");
            }
            if (hasColumn(rs, "closing_price_day")) {
                String formattedPrice = nf.format(rs.getDouble("closing_price_day"));
                buffer.append(" (day " + formattedPrice + ")");
            }

            System.out.println(buffer.toString());

        }
    }

    public static boolean hasColumn(ResultSet rs, String column_name) throws SQLException {
        try
        {
            rs.findColumn(column_name);
            return true;
        } catch (SQLException e)
        {
            //System.err.println("Column does not exist: " + e);
        }
        return false;
    }
//
//    public static void displayTotalTrades(ResultSet rs) throws SQLException {
//        rs.last();
//        if (rs.getRow() == 0) {
//            System.out.println("No data found");
//        }
//
//        rs.beforeFirst();
//        rs.next();
//        System.out.println(rs.getInt("total"));
//    }

    public static String getMaxMinAndVolumeForGivenDate(String stock_symbol, String date) {
        //String query = "select symbol, price from " + TABLE_NAME +  " where price = (select max(price) from stock_info where date = '" + date + "');";
        String query;
        if (stock_symbol.equals("all")) {
            query = "select symbol, max(price) as max_price, min(price) as min_price, sum(volume) as total from " + TABLE_NAME + " where date like '" + date + "%' group by symbol;";
        } else {
            query = "select t2.symbol, t2.max_price, t2.min_price, t2.total from " +
                    "(select symbol, max(price) as max_price, min(price) as min_price, sum(volume) as total from " + TABLE_NAME + " where date like '" + date + "%' group by symbol) " +
                    "as t2 where symbol = '" + stock_symbol + "';";
        }
        return query;
    }


    public static String getClosingPriceForGivenDate(String stock_symbol, String date) {
        String query;
        if (stock_symbol.equals("all")) {
            query = "select t1.symbol, t1.price as closing_price_day, t2.price as closing_price_month from " +
                    "(select symbol, price from " + TABLE_NAME + " where date = (select max(date) from " + TABLE_NAME + " where date like '" + date + "%')) as t1 " +
                    "join " +
                    "(select symbol, price from " + TABLE_NAME + " where date = (select max(date) from " + TABLE_NAME + " where date like '" + date.substring(0,7) + "%') ) as t2 " +
                    "where t1.symbol = t2.symbol;";
        } else {
            query = "select t1.symbol, t1.price as closing_price_month, t2.price as closing_price_day from " +
                    "(select symbol, price from " + TABLE_NAME + " where id = (select max(id) from " + TABLE_NAME + " where symbol = '" + stock_symbol + "' and date like '" + date.substring(0,7) + "%')) as t1 " +
                    "join (select symbol, price from " + TABLE_NAME + " where id = (select max(id) from " + TABLE_NAME + " where symbol = '" + stock_symbol + "' and date like '" + date + "%')) as t2 " +
                    "on t1.symbol = t2.symbol;";
        }
        return query;
    }
}
