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
     * @param header Must contain connection information: ['connection-url', 'connection-user', 'connection-password']
     */
    public JDBCTable(Header header) {
        super(header);
        validateHeaderConnection(header);
    }

    public JDBCTable(Header header, String connectionUrl, String connectionUser, String connectionPassword) {
        super(header);
        this.header.set("connection-url", connectionUrl);
        this.header.set("connection-user", connectionUser);
        this.header.set("connection-password", connectionPassword);
    }

    private void validateHeaderConnection(Header header) {
        ArrayList<String> missingConnectionFields = new ArrayList<>();
        if (header.get("connection-url") == null) missingConnectionFields.add("connection-url");
        if (header.get("connection-user") == null) missingConnectionFields.add("connection-user");
        if (header.get("connection-password") == null) missingConnectionFields.add("connection-password");

        if (!missingConnectionFields.isEmpty()) {
            throw new DataBaseException("JDBCTable", "Header must contain: " + String.join(", ", missingConnectionFields));
        }
    }

    @Override
    public void open() {
        try {
            if (this.connection == null || this.connection.isClosed()) {
                String connectionUrl = header.get("connection-url");
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
        } catch (DataBaseException | SQLException e) {
            throw new DataBaseException("JDBCTable", e.getMessage());
        }

    }

    protected void setPrototype() {
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
            translatorApi = header.getPrototype().validateColumns();
        } catch (DataBaseException | SQLException e) {
            throw new DataBaseException("JDBCTable", e.getMessage());
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
        } catch (SQLException ignored) {
        }
    }

    @Override
    protected RowIterator<Long> iterator(List<String> columns, Long lowerbound) {
        return new RowIterator<>() {
            long currentIt = 0L;
            ResultSet results;

            {
                fetchResults();
            }

            private void fetchResults() {
                try {
                    String selectedColumns = "*";
                    if (columns != null) {
                        selectedColumns = columns.toString();
                        selectedColumns = selectedColumns.substring(1, selectedColumns.length() - 1);
                    }

                    PreparedStatement ps = getSelectStatement(selectedColumns, lowerbound);
                    results = ps.executeQuery();
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                }
            }

            @Override
            public void restart() {
                currentIt = 0L;
                fetchResults();
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
                try {
                    return results.next();
                } catch (SQLException e) {
                    throw new DataBaseException("JDBCTable", e.getMessage());
                }
            }

            @Override
            public RowData next() {
                try {
                    currentIt++;

                    RowData rowData = new RowData();
                    for (Column c : header.getPrototype().getColumns()) {
                        if (columns != null && !columns.contains(c.getName())) {
                            continue;
                        }

                        String val = results.getString(c.getName());

                        if (val == null || val.compareToIgnoreCase("null") == 0 || val.isEmpty() || val.isBlank()) {
                            rowData.setField(c.getName(), new NullField(c), c);
                            continue;
                        }

                        switch (Util.typeOfColumn(c)) {
                            case "string":
                                rowData.setString(c.getName(), val, c);
                                break;
                            case "int":
                                rowData.setInt(c.getName(), Integer.parseInt(val), c);
                                break;
                            case "long":
                                rowData.setLong(c.getName(), Long.valueOf(val), c);
                                break;
                            case "double":
                                rowData.setDouble(c.getName(), Double.parseDouble(val), c);
                                break;
                            case "float":
                                rowData.setFloat(c.getName(), Float.parseFloat(val), c);
                                break;
                            case "boolean":
                                rowData.setBoolean(c.getName(), Boolean.parseBoolean(val), c);
                                break;
                        }
                    }

                    return rowData;
                } catch (Exception e) {
                    throw new RuntimeException(e);
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

    /**
     * @param selectedColumns Columns to be retrieved in the SELECT statement. e.g: "id, name"
     */
    protected PreparedStatement getSelectStatement(String selectedColumns, Long offset) {
        try {
            Long registersCount = this.getRowCount();
            String query = "SELECT " + selectedColumns +
                    " FROM " + header.get(Header.TABLE_NAME) +
                    " LIMIT ?, ?";

            PreparedStatement ps = connection.prepareStatement(query);
            ps.setLong(1, offset);
            ps.setLong(2, registersCount);

            return ps;
        } catch (SQLException e) {
            throw new DataBaseException("JDBCTable", e.getMessage());
        }
    }

    protected Long getRowCount() {
        try {
            String query = "SELECT COUNT(*) FROM " + header.get(Header.TABLE_NAME);
            PreparedStatement ps = connection.prepareStatement(query);

            try (ResultSet resultSet = ps.executeQuery()) {
                long rowCount = 0L;
                if (resultSet.next()) {
                    rowCount = resultSet.getLong(1);
                }

                ps.close();
                resultSet.close();

                return rowCount;
            }
        } catch (SQLException e) {
            throw new DataBaseException("JDBCTable", e.getMessage());
        }
    }

}
