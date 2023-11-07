package sgbd.source.table;

import sgbd.source.components.Header;

public class PostgreSQLTable extends JDBCTable {

    public PostgreSQLTable(Header header, String connectionUrl) {
        super(header, connectionUrl);
        this.header.set("connectionType", "PostgreSQL");
    }

    public PostgreSQLTable(Header header, String connectionUrl, String connectionUser, String connectionPassword) {
        super(header, connectionUrl, connectionUser, connectionPassword);
        this.header.set("connectionType", "PostgreSQL");
    }

}