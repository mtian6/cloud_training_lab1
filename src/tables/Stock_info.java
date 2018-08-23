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
            if (method == "max") {
                items_map.put("max_price", rs.getString("price"));
            } else if (method == "min") {
                items_map.put("min_price", rs.getString("price"));
            } else if (method == "total") {
                items_map.put("total_volume", rs.getString("total"));
            } else if (method == "closing") {
                items_map.put("closing_price_day", rs.getString("closing_price_day"));
                items_map.put("closing_price_month", rs.getString("closing_price_month"));
            }
            items_map.put("date", date);
//            if (hasColumn(rs, "price")) {
//                Map items_map = (Map)symbol_map.get(sym);
//                items_map.put("price", rs.getString("price"));
//            } else if (hasColumn(rs, "total")) {
//                Map items_map = (Map)symbol_map.get(sym);
//                items_map.put("total", rs.getString("total"));
//            }
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
            if (hasColumn(rs, "price")) {
                String formattedPrice = nf.format(rs.getDouble("price"));
                buffer.append(" (" + formattedPrice + ")");
            }
            if (hasColumn(rs, "total")) {
                Double totalVolume = rs.getDouble("total");
                buffer.append(" (" + totalVolume + ")");
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

    public static boolean hasColumn(ResultSet rs, String columnName) throws SQLException {
        try
        {
            rs.findColumn(columnName);
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

    public static String getHighestPriceForGivenDate(String name, String date) {
        //String query = "select symbol, price from " + TABLE_NAME +  " where price = (select max(price) from stock_info where date = '" + date + "');";
        String query;
        if (name.equals("all")) {
            query = "select symbol, max(price) as price from " + TABLE_NAME + " where date = '" + date + "' group by symbol;"; //max price for given day for each stock symbol; "
        } else {
            query = "select t2.symbol, t2.price from (select symbol, max(price) as price from " + TABLE_NAME + " where date = '" + date + "' group by symbol) as t2 where symbol = '" + name + "';";
        }
        return query;
    }

    public static String getLowestPriceForGivenDate(String name, String date) {
        //String query = "select symbol, price from " + TABLE_NAME +  "  where price = (select min(price) from stock_info where date = '" + date + "');";
        String query;
        if (name.equals("all")) {
            query = "select symbol, min(price) as price from " + TABLE_NAME + " where date = '" + date + "' group by symbol;"; //min price for given day for each stock symbol; "
        } else {
            query = "select t2.symbol, t2.price from (select symbol, min(price) as price from " + TABLE_NAME + " where date = '" + date + "' group by symbol) as t2 where symbol = '" + name + "';";
        }
        return query;
    }

    public static String getTotalVolumeTradedForGivenDate(String name, String date) {
        String query;
        if (name.equals("all")) {
            query = "select symbol, sum(volume) as total from " + TABLE_NAME + " where date = '" + date + "' group by symbol;";
        } else {
            query = "select symbol, sum(volume) as total from " + TABLE_NAME + " where date = '" + date + "' and symbol = '" + name + "';";
        }
        return query;
    }

    public static String getClosingPriceForGivenDate(String name, String date) {
        String query;
        if (name.equals("all")) {
            query = "select t1.symbol, t1.price as closing_price_month, t2.price as closing_price_day from " +
                    "(select " + TABLE_NAME + ".symbol, price from " + TABLE_NAME +
                    " join (select symbol, max(id) as maxid from " + TABLE_NAME +
                    " where date like '" + date.substring(0,7) + "%' group by symbol)" +
                    " as t2 where stock_info.id = t2.maxid) as t1 join " +
                    "(select " + TABLE_NAME + ".symbol, price from " + TABLE_NAME +
                    " join (select symbol, max(id) as maxid from " + TABLE_NAME +
                    " where date = '" + date + "' group by symbol)" +
                    " as t2 where stock_info.id = t2.maxid) as t2 where t1.symbol = t2.symbol;";
        } else {
            query = "select t3.symbol, t3.price from (select " + TABLE_NAME + ".symbol, price from " + TABLE_NAME +
                    " join (select symbol, max(id) as maxid from " + TABLE_NAME +
                    " where date = '" + date + "' group by symbol)" +
                    " as t2 where stock_info.id = t2.maxid) as t3 where t3.symbol = '" + name + "';";
        }
        return query;
    }
}
