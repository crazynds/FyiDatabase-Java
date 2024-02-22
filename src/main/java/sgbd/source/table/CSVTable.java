package sgbd.source.table;

import engine.exceptions.DataBaseException;
import lib.BigKey;
import sgbd.prototype.column.Column;
import sgbd.prototype.RowData;
import sgbd.prototype.query.fields.NullField;
import sgbd.source.components.Header;
import sgbd.source.components.RowIterator;
import sgbd.util.classes.CSVRecognizer;
import sgbd.util.classes.InvalidCsvException;
import sgbd.util.global.Util;


import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CSVTable extends Table {
    private CSVRecognizer recognizer;
    private char separator,stringDelimiter;
    private int beginIndex;

    private static final String pkName = "__IDX__";

    private static Header prepareStuff(Header header){
        for (Column c:header.getPrototype()) {
            if(c.getName().compareTo(pkName)==0)return header;
        }
        header.getPrototype().addColumn(CSVTable.pkName,8,Column.PRIMARY_KEY|Column.IGNORE_COLUMN);
        return header;
    }

    public CSVTable(Header header) {
        super(prepareStuff(header));
        this.beginIndex = Integer.parseInt(header.get("beginIndex"));
        this.separator = header.get("separator").charAt(0);
        this.stringDelimiter = header.get("delimiter").charAt(0);
    }
    public CSVTable(Header header,char separator, char stringDelimiter, int beginIndex) {
        super(prepareStuff(header));
        header.set(Header.TABLE_TYPE,"CSVTable");
        header.set("separator",separator+"");
        header.set("delimiter",stringDelimiter+"");
        header.set("beginIndex",beginIndex+"");
        this.beginIndex = beginIndex;
        this.separator = separator;
        this.stringDelimiter = stringDelimiter;
    }

    @Override
    public void clear() {
        throw new DataBaseException("CSVTable","This type of table (CSVTable) is not writable");
    }

    @Override
    public void open() {
        try {
            this.recognizer = new CSVRecognizer(header.getTablePath(),separator,stringDelimiter,beginIndex);
        } catch (InvalidCsvException e) {
            throw new DataBaseException("CSVTable->Constructor",e.getMessage());
        }
    }

    @Override
    public RowData findByRef(RowData reference) {
        return null;
    }

    @Override
    public void close() {
        this.recognizer = null;
    }


    @Override
    public void insert(RowData r) {
        throw new DataBaseException("CSVTable","This type of table (CSVTable) is not writable");
    }

    @Override
    public void insert(List<RowData> r) {
        throw new DataBaseException("CSVTable","This type of table (CSVTable) is not writable");
    }

    @Override
    public RowIterator iterator(List<String> columns) {
        return this.iterator(columns, null);
    }

    @Override
    protected RowIterator iterator(List<String> columns, RowData lowerbound) {
        return new RowIterator() {
            RowIterator sub = iterator();
            Map<String, Column> metaInfo = translatorApi.generateMetaInfo(columns);

            {
                //for(int x=0;x<lowerbound;x++)sub.next();
            }

            @Override
            public void restart() {
                sub.restart();
                //for(int x=0;x<lowerbound;x++)sub.next();
            }

            @Override
            public void unlock() {
                sub.unlock();
            }

            @Override
            public boolean hasNext() {
                return sub.hasNext();
            }

            @Override
            public RowData next() {
                return sub.next();
            }
        };
    }

    @Override
    public RowIterator iterator() {
        return new RowIterator() {

            long currentIt = 0L;
            Iterator<String[]> csvLines = recognizer.iterator();
            String[] headers = recognizer.getColumnNames();

            @Override
            public void unlock() {
            }

            @Override
            public void restart() {
                currentIt = 0L;
                csvLines = recognizer.iterator();
            }

            @Override
            public boolean hasNext() {
                return csvLines.hasNext();
            }

            @Override
            public RowData next() {
                String[] data = csvLines.next();
                if(data==null)return null;
                currentIt += 1;

                String[] columns = recognizer.getColumnNames();
                RowData rowData = new RowData();
                for (Column c:
                        getHeader().getPrototype()) {
                    if(c.getName().compareTo(CSVTable.pkName)==0){
                        rowData.setLong(c.getName(),currentIt,c);
                        continue;
                    }
                    for (int x=0;x<columns.length;x++) {
                        String columnName = columns[x];
                        if(c.getName().compareToIgnoreCase(columnName) != 0)continue;
                        String val = data[x];
                        if(val == null || val.compareToIgnoreCase("null")==0 || val.isEmpty() || val.strip().isEmpty()){
                            rowData.setField(c.getName(),new NullField(c),c);
                            continue;
                        }

                        switch (Util.typeOfColumn(c)){
                            case "string":
                                rowData.setString(c.getName(),val,c);
                                break;
                            case "int":
                                rowData.setInt(c.getName(),Integer.valueOf(val),c);
                                break;
                            case "long":
                                rowData.setLong(c.getName(),Long.valueOf(val),c);
                                break;
                            case "double":
                                rowData.setDouble(c.getName(),Double.valueOf(val),c);
                                break;
                            case "float":
                                rowData.setFloat(c.getName(),Float.valueOf(val),c);
                                break;
                            case "boolean":
                                rowData.setBoolean(c.getName(),Boolean.valueOf(val),c);
                                break;
                        }

                    }
                }
                return rowData;
            }
        };
    }
}
