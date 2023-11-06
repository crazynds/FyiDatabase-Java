package sgbd.source.table;

import engine.exceptions.DataBaseException;
import lib.BigKey;
import sgbd.prototype.RowData;
import sgbd.source.components.Header;
import sgbd.source.components.RowIterator;

import java.sql.*;
import java.util.*;

abstract public class JDBCTable extends Table {

    public Connection connection;
    public Set<String> pkColumns;
    /**
     * Page size used in iterator for
     * fetching results
     */
    public int pageSize = 100;

    public JDBCTable(Header header, String connectionUrl) {
        super(header);
        this.header.set("connectionUrl", connectionUrl);
    }

    public JDBCTable(Header header, String connectionUrl, String connectionUser, String connectionPassword) {
        this(header, connectionUrl);
        this.header.set("connectionUser", connectionUser);
        this.header.set("connectionPassword", connectionPassword);
    }

    @Override
    public void open() {
        String connectionUrl = header.get("connectionUrl");
        if (connectionUrl != null) {
            try {
                String connectionUser = header.get("connectionUser");
                String connectionPassword = header.get("connectionPassword");
                if (connectionUser != null && connectionPassword != null) {
                    connection = DriverManager.getConnection(connectionUrl, connectionUser, connectionPassword);
                } else {
                    connection = DriverManager.getConnection(connectionUrl);
                }

                setTablePrimaryKeys();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public void setTablePrimaryKeys() {
        try {
            ResultSet pkColumns = connection.getMetaData().getPrimaryKeys(null, null, header.get(Header.TABLE_NAME));
            SortedSet<String> pkColumnSet = new TreeSet<>();
            while (pkColumns.next()) {
                String pkColumnName = pkColumns.getString("COLUMN_NAME");
                pkColumnSet.add(pkColumnName);
            }

            this.pkColumns = pkColumnSet;
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void clear() {
        throw new DataBaseException("JDBCTable", "This type of table (JDBCTable) is not writable");
    }

    @Override
    public BigKey insert(RowData r) {
        throw new DataBaseException("JDBCTable", "This type of table (JDBCTable) is not writable");
    }

    @Override
    public void insert(List<RowData> r) {
        throw new DataBaseException("JDBCTable", "This type of table (JDBCTable) is not writable");
    }


    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

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
            long currentPage = 1;
            int registersCount = getRegistersCount();
            int currentIndex = 0;
            final int pageSize = getPageSize();
            ResultSet results = fetchResults();

            private ResultSet fetchResults() {
                try {
                    String pkColumn = pkColumns.stream().findFirst().orElseThrow();
                    // FIXME: Avoid SQL Injection && Only works with 1 PK Column
                    String query = "SELECT * FROM " + header.get(Header.TABLE_NAME) + " ORDER BY ? LIMIT ? OFFSET ?";
                    PreparedStatement ps = connection.prepareStatement(query);
                    ps.setString(1, pkColumn);
                    ps.setInt(2, pageSize);
                    ps.setLong(3, (currentPage - 1) * pageSize);

                    return ps.executeQuery();
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                    return null;
                }
            }

            private int getRegistersCount() {
                try {
                    String query = "SELECT COUNT(1) AS row_count FROM " + header.get(Header.TABLE_NAME);
                    PreparedStatement ps = connection.prepareStatement(query);
                    ResultSet rs = ps.executeQuery();

                    if (rs.next()) {
                        return rs.getInt("row_count");
                    }

                    return 0;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return 0;
                }
            }

            @Override
            public void restart() {
                currentPage = 1;
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
                        if (currentIndex < registersCount) {
                            currentPage++;
                            results = this.fetchResults();
                            return this.hasNext();
                        }

                        registersCount = 0;
                        results.close();
                    }

                    return hasNext;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return false;
                }
            }

            @Override
            public RowData next() {
                if (registersCount > 0) {
                    try {
                        RowData rowData = new RowData();
                        ResultSetMetaData metaData = results.getMetaData();
                        int columnCount = metaData.getColumnCount();
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = metaData.getColumnName(i);
                            int columnType = metaData.getColumnType(i);
                            String columnValue = "";

                            Object valueObject = results.getObject(i);
                            if (valueObject != null) {
                                columnValue = valueObject.toString();
                            }

                            switch (columnType) {
                                case Types.VARCHAR:
                                    rowData.setString(columnName, columnValue);
                                    break;
                                case Types.INTEGER:
                                    rowData.setInt(columnName, Integer.parseInt(columnValue));
                                    break;
                                case Types.FLOAT:
                                    rowData.setFloat(columnName, Float.parseFloat(columnValue));
                                    break;
                                case Types.DOUBLE:
                                    rowData.setDouble(columnName, Double.parseDouble(columnValue));
                                case Types.BOOLEAN:
                                    rowData.setBoolean(columnName, Boolean.parseBoolean(columnValue));
                                    break;
                            }
                        }

                        currentIndex++;
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

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getPageSize() {
        return pageSize;
    }

}
