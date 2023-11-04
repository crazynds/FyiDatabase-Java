package sgbd.source.table;

import lib.BigKey;
import sgbd.prototype.RowData;
import sgbd.source.components.RowIterator;
import sgbd.source.components.Header;

import java.sql.*;
import java.util.*;

public class MySQLTable extends Table {

    public final Connection connection;
    public final String tableName;
    public Set<String> pkColumns;

    public MySQLTable(Header header, Connection connection, String tableName) throws SQLException {
        super(header);
        this.connection = connection;
        this.tableName = tableName;
        this.pkColumns = getPrimaryKeyColumnsForTable(connection, tableName);
    }

    public static Set<String> getPrimaryKeyColumnsForTable(Connection connection, String tableName) throws SQLException {
        try (ResultSet pkColumns = connection.getMetaData().getPrimaryKeys(null, null, tableName);) {
            SortedSet<String> pkColumnSet = new TreeSet<>();
            while (pkColumns.next()) {
                String pkColumnName = pkColumns.getString("COLUMN_NAME");
                pkColumnSet.add(pkColumnName);
            }
            return pkColumnSet;
        }
    }

    @Override
    public void clear() {

    }

    @Override
    public void open() {

    }

    @Override
    public void close() {

    }

    @Override
    protected RowIterator<Long> iterator(List<String> columns, Long lowerbound) {
        return null;
    }

    @Override
    public RowIterator<Long> iterator(List<String> columns) {
        return null;
    }

    @Override
    public RowIterator<Long> iterator() {
        return new RowIterator<Long>() {
            long currentIt = 1;
            int currentPageSize = 0;
            final int pageSize = 20;
            ResultSet results = fetchResults();

            private ResultSet fetchResults() {
                try {
                    String pkColumn = pkColumns.stream().findFirst().orElseThrow();
                    // FIXME: Avoid SQL Injection && Only works with 1 PK Column
                    String query = "SELECT * FROM " + tableName + " ORDER BY ? LIMIT ? OFFSET ?";
                    PreparedStatement ps = connection.prepareStatement(query);
                    ps.setString(1, pkColumn);
                    ps.setInt(2, pageSize);
                    ps.setLong(3, (currentIt - 1) * pageSize);

                    return ps.executeQuery();
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                    return null;
                }
            }

            @Override
            public void restart() {
                currentIt = 1;
                results = fetchResults();
            }

            @Override
            public void unlock() {
            }

            @Override
            public Long getRefKey() {
                return null;
            }

            @Override
            public boolean hasNext() {
                try {
                    boolean hasNext = results.next();
                    if (!hasNext) {
                        currentPageSize = 0;
                        results.close();
                    } else {
                        currentPageSize++;
                    }
                    return hasNext;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return false;
                }
            }

            @Override
            public RowData next() {
                if (currentPageSize > 0) {
                    try {
                        RowData rowData = new RowData();
                        ResultSetMetaData metaData = results.getMetaData();
                        int columnCount = metaData.getColumnCount();
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = metaData.getColumnName(i);
                            String columnType = metaData.getColumnTypeName(i);
                            String columnValue = "";

                            Object valueObject = results.getObject(i);
                            if (valueObject != null) {
                                columnValue = valueObject.toString();
                            }

                            switch (columnType){
                                case "VARCHAR":
                                    rowData.setString(columnName, columnValue);
                                    break;
                                case "INTEGER":
                                    rowData.setInt(columnName, Integer.parseInt(columnValue));
                                    break;
                            }
                        }

                        return rowData;
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    throw new NoSuchElementException();
                }
            }
        };
    }

    @Override
    public BigKey insert(RowData r) {
        return null;
    }

    @Override
    public void insert(List<RowData> r) {

    }
}
