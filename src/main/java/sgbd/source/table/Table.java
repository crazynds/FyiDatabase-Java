package sgbd.source.table;

import java.io.IOException;

import java.util.List;

import engine.exceptions.DataBaseException;
import lib.BigKey;
import sgbd.prototype.RowData;
import sgbd.query.Operator;
import sgbd.source.Source;
import sgbd.source.components.Header;
import sgbd.source.components.RowIterator;
import sgbd.source.index.Index;

public abstract class Table extends Source {

	public Table(Header header)  {
		super(header);
	}

	public static Table openTable(Header header){
		return SimpleTable.openTable(header,false);
	}
	public static Table openTable(Header header, boolean clear){
		header.setBool("clear",clear);
		if(header.get(Header.TABLE_TYPE)==null)return new SimpleTable(header);
        return switch (header.get(Header.TABLE_TYPE)) {
			case "MemoryTable" -> new MemoryTable(header);
			case "CompleteTable" -> new CompleteTable(header);
            case "CSVTable" -> new CSVTable(header);
            case "MySQLTable" -> new MySQLTable(header);
            case "PostgreSQLTable" -> new PostgreSQLTable(header);
            case "OracleTable" -> new OracleTable(header);
            default -> new SimpleTable(header);
        };
	}
	public static Table loadFromHeader(String headerPath){
		Header header;
		try {
			header = Header.load(headerPath);
		}catch (IOException ex){
			throw new DataBaseException("Table->saveHeader",ex.getMessage());
		}
		return openTable(header);
	}

	/*
		Retorna nome da table
	 */
	public String getSourceName(){
		return header.get(Header.TABLE_NAME);
	}

}
