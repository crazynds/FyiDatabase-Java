package sgbd.source.table;

import engine.exceptions.DataBaseException;
import lib.BigKey;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.prototype.column.*;
import sgbd.prototype.query.fields.NullField;
import sgbd.source.components.Header;
import sgbd.source.components.RowIterator;
import sgbd.util.global.Util;

import java.sql.*;
import java.util.*;

abstract public class JDBCTable extends Table {

    public Connection connection;

    /**
     * Page size used in iterator for
     * fetching results
     */
    public int pageSize = 100;

    /**
     * @param header Must contain connection information: ['connection-url', 'connection-user', 'connection-password']
     */
    public JDBCTable(Header header) {
        super(header);
    }

    public JDBCTable(Header header, String connectionUrl) {
        super(header);
        this.header.set("connection-url", connectionUrl);
    }

    public JDBCTable(Header header, String connectionUrl, String connectionUser, String connectionPassword) {
        this(header, connectionUrl);
        this.header.set("connection-user", connectionUser);
        this.header.set("connection-password", connectionPassword);
    }

    @Override
    public void open() {
        String connectionUrl = header.get("connection-url");
        if (connectionUrl != null) {
            try {
                if (this.connection == null || this.connection.isClosed()) {
                    String connectionUser = header.get("connection-user");
                    String connectionPassword = header.get("connection-password");
                    if (connectionUser != null && connectionPassword != null) {
                        connection = DriverManager.getConnection(connectionUrl, connectionUser, connectionPassword);
                    } else {
                        connection = DriverManager.getConnection(connectionUrl);
                    }
                }

                // Forcefully rewrite header's prototype
                setPrototype();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public void setPrototype() {
        Prototype pt = new Prototype();

        try {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columns = metaData.getColumns(null, null, header.get(Header.TABLE_NAME), null);
            ResultSet pkColumns = metaData.getPrimaryKeys(null, null, header.get(Header.TABLE_NAME));
            ArrayList<String> pkColumnsNames = new ArrayList<>();

            while (pkColumns.next()) {
                pkColumnsNames.add(pkColumns.getString("COLUMN_NAME"));
            }

            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                int columnType = Integer.parseInt(columns.getString("DATA_TYPE"));
                boolean isPk = pkColumnsNames.contains(columnName);

                switch (columnType) {
                    case Types.INTEGER:
                        pt.addColumn(new IntegerColumn(columnName, isPk));
                        break;
                    case Types.BIGINT:
                        pt.addColumn(new LongColumn(columnName, isPk));
                        break;
                    case Types.FLOAT:
                        pt.addColumn(new FloatColumn(columnName));
                        break;
                    case Types.DOUBLE:
                        pt.addColumn(new DoubleColumn(columnName));
                        break;
                    case Types.BOOLEAN:
                        pt.addColumn(new BooleanColumn(columnName));
                        break;
                    default:
                        // TODO: Map all values to correct data types
                        pt.addColumn(new StringColumn(columnName));
                }
            }

            header.setPrototype(pt);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ArrayList<String> getPKColumns() {
        ArrayList<String> pkColumns = new ArrayList<>();
        for (Column column : header.getPrototype().getColumns()) {
            if (column.isPrimaryKey()) {
                pkColumns.add(column.getName());
            }
        }

        return pkColumns;
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
        return new RowIterator<>() {
            long currentIt = 0L;
            long registersCount = 0;
            long lastPage;
            long currentPage = 0L;
            long pageSize;
            ResultSet results;

            {
                this.updatePagination();
                this.results = fetchResults();
            }

            private ResultSet fetchResults() {
                try {
                    String selectedColumns = "*";
                    if (columns != null) {
                        columns.addAll(getPKColumns());
                        selectedColumns = columns.toString();
                        selectedColumns = selectedColumns.substring(1, selectedColumns.length() - 1);
                    }

                    Long offset = (currentPage - 1) * pageSize + lowerbound;
                    PreparedStatement ps = getStatementForPaginatedSelect(selectedColumns, pageSize, offset);
                    return ps.executeQuery();
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                }

                return null;
            }

            private void updatePagination() {
                currentPage++;
                this.pageSize = getPageSize();
                try {
                    String query = "SELECT COUNT(1) AS row_count FROM " + header.get(Header.TABLE_NAME);
                    PreparedStatement ps = connection.prepareStatement(query);
                    ResultSet rs = ps.executeQuery();

                    if (rs.next()) {
                        this.registersCount = (rs.getInt("row_count") - lowerbound);
                        this.lastPage = (long) Math.ceil((double) registersCount / pageSize);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void restart() {
                currentIt = 0L;
                currentPage = 1L;
                results = fetchResults();
            }

            @Override
            public void unlock() {
            }

            @Override
            public Long getRefKey() {
                return currentIt;
            }

            @Override
            public boolean hasNext() {
                if (currentIt < registersCount) {
                    if (currentIt == pageSize * currentPage) {
                        this.updatePagination();
                        results = this.fetchResults();
                    }

                    return true;
                }

                registersCount = 0;
                return false;
            }

            @Override
            public RowData next() {
                if (registersCount > 0) {
                    try {
                        results.next();
                        currentIt++;

                        RowData rowData = new RowData();
                        for (Column c : header.getPrototype().getColumns()) {
                            if (columns != null && !columns.contains(c.getName())) {
                                continue;
                            }

                            String val = results.getString(c.getName());

                            if(val == null || val.compareToIgnoreCase("null")==0 || val.isEmpty() || val.isBlank()){
                                rowData.setField(c.getName(),new NullField(c),c);
                                continue;
                            }

                            switch (Util.typeOfColumn(c)){
                                case "string":
                                    rowData.setString(c.getName(),val,c);
                                    break;
                                case "int":
                                    rowData.setInt(c.getName(),Integer.parseInt(val),c);
                                    break;
                                case "long":
                                    rowData.setLong(c.getName(),Long.valueOf(val),c);
                                    break;
                                case "double":
                                    rowData.setDouble(c.getName(),Double.parseDouble(val),c);
                                    break;
                                case "float":
                                    rowData.setFloat(c.getName(),Float.parseFloat(val),c);
                                    break;
                                case "boolean":
                                    rowData.setBoolean(c.getName(),Boolean.parseBoolean(val),c);
                                    break;
                            }

                        }

                        return rowData;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    throw new NoSuchElementException();
                }
            }
        };
    }

    @Override
    public RowIterator<Long> iterator(List<String> columns) {
        return this.iterator(columns, 0L);
    }

    @Override
    public RowIterator<Long> iterator() {
        return this.iterator(null, 0L);
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getPageSize() {
        return pageSize;
    }

    /**
     * @param selectedColumns Columns to be retrieved in the SELECT statement. e.g: "id, name"
     */
    protected PreparedStatement getStatementForPaginatedSelect(String selectedColumns, Long pageSize, Long offset) {
        try {
            String query = "SELECT " + selectedColumns +
                    " FROM " + header.get(Header.TABLE_NAME) +
                    " LIMIT ? OFFSET ?";

            PreparedStatement ps = connection.prepareStatement(query);
            ps.setLong(1, pageSize);
            ps.setLong(2, offset);

            return ps;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

}
