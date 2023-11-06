package sgbd.source.table;

import sgbd.source.components.Header;

public class MySQLTable extends JDBCTable {

    public MySQLTable(Header header, String connectionUrl) {
        super(header, connectionUrl);
        this.header.set("connectionType", "MySQL");
    }

    public MySQLTable(Header header, String connectionUrl, String connectionUser, String connectionPassword) {
        super(header, connectionUrl, connectionUser, connectionPassword);
        this.header.set("connectionType", "MySQL");
    }
}