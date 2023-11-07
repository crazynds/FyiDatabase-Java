package sgbd.source.table;

import sgbd.source.components.Header;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class OracleTable extends JDBCTable {

    public OracleTable(Header header, String connectionUrl) {
        super(header, connectionUrl);
        this.header.set("connectionType", "PostgreSQL");
    }

    public OracleTable(Header header, String connectionUrl, String connectionUser, String connectionPassword) {
        super(header, connectionUrl, connectionUser, connectionPassword);
        this.header.set("connectionType", "PostgreSQL");
    }

    /**
     * TODO: Review
     *
     * @param selectedColumns Columns to be retrieved in the SELECT statement. e.g: "id, name"
     */
    @Override
    protected PreparedStatement getStatementForPaginatedSelect(String selectedColumns, Long pageSize, Long offset) {
        try {
            String query = "SELECT " + selectedColumns +
                    " FROM " + header.get(Header.TABLE_NAME) +
                    " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

            PreparedStatement ps = connection.prepareStatement(query);
            ps.setLong(1, offset);
            ps.setLong(2, pageSize);

            return ps;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

}