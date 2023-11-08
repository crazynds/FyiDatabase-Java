package sgbd.source.table;

import sgbd.source.components.Header;

public class OracleTable extends JDBCTable {

    public OracleTable(Header header)
    {
        super(header);
        this.header.set(Header.TABLE_TYPE, "OracleTable");
    }

    public OracleTable(Header header, String connectionUrl, String connectionUser, String connectionPassword) {
        super(header, connectionUrl, connectionUser, connectionPassword);
    }

}