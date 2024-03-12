package sgbd.source.table;

import sgbd.prototype.RowData;
import sgbd.source.components.Header;

public class MySQLTable extends JDBCTable {

    public MySQLTable(Header header)
    {
        super(header);
        this.header.set(Header.TABLE_TYPE, "MySQLTable");
    }

    public MySQLTable(Header header, String connectionUrl, String connectionUser, String connectionPassword) {
        super(header, connectionUrl, connectionUser, connectionPassword);
    }

}