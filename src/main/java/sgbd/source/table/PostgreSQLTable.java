package sgbd.source.table;

import sgbd.source.components.Header;

public class PostgreSQLTable extends JDBCTable {

    public PostgreSQLTable(Header header)
    {
        super(header);
        this.header.set(Header.TABLE_TYPE, "PostgreSQLTable");
    }

    public PostgreSQLTable(Header header, String connectionUrl, String connectionUser, String connectionPassword) {
        super(header, connectionUrl, connectionUser, connectionPassword);
    }

}