package sgbd.table;

import engine.exceptions.DataBaseException;
import sgbd.prototype.column.Column;
import sgbd.prototype.RowData;
import sgbd.prototype.query.fields.NullField;
import sgbd.table.components.Header;
import sgbd.table.components.RowIterator;
import sgbd.util.classes.CSVRecognizer;
import sgbd.util.classes.InvalidCsvException;
import sgbd.util.global.Util;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CSVTable extends Table{
    private CSVRecognizer recognizer;
    private char separator,stringDelimiter;
    private int beginIndex;

    private static final String pkName = "__IDX__";

    private static Header prepareStuff(Header header){
        header.getPrototype().addColumn(CSVTable.pkName,8,Column.PRIMARY_KEY);
        return header;
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
    public void close() {
        this.recognizer = null;
    }

    @Override
    public BigInteger insert(RowData r) {
        throw new DataBaseException("CSVTable","This type of table (CSVTable) is not writable");
    }

    @Override
    public void insert(List<RowData> r) {
        throw new DataBaseException("CSVTable","This type of table (CSVTable) is not writable");
    }

    @Override
    public RowData find(BigInteger pk) {
        for (RowIterator it = this.iterator(); it.hasNext(); ) {
            Map.Entry<BigInteger, RowData> row = it.nextWithPk();
            if(row.getKey().compareTo(pk) == 0)return row.getValue();
        }
        return null;
    }

    @Override
    public RowData find(BigInteger pk, List<String> columns) {
        for (RowIterator it = this.iterator(columns); it.hasNext(); ) {
            Map.Entry<BigInteger, RowData> row = it.nextWithPk();
            if(row.getKey().compareTo(pk) == 0)return row.getValue();
        }
        return null;
    }

    @Override
    public RowData update(BigInteger pk, RowData r) {
        throw new DataBaseException("CSVTable","This type of table (CSVTable) is not writable");
    }

    @Override
    public RowData delete(BigInteger pk) {
        throw new DataBaseException("CSVTable","This type of table (CSVTable) is not writable");
    }

    @Override
    public RowIterator iterator(List<String> columns) {
        return new RowIterator() {
            RowIterator sub = iterator();
            Map<String, Column> metaInfo = translatorApi.generateMetaInfo(columns);
            @Override
            public void setPointerPk(BigInteger pk) {
                sub.setPointerPk(pk);
            }

            @Override
            public void restart() {
                sub.restart();
            }

            @Override
            public void unlock() {
                sub.unlock();
            }

            @Override
            public Map.Entry<BigInteger, RowData> nextWithPk() {
                Map.Entry<BigInteger, RowData> comp = sub.nextWithPk();
                if(comp==null)
                    return null;
                return Map.entry(comp.getKey(),translatorApi.convertToRowData(translatorApi.convertToRecord(comp.getValue()),metaInfo));
            }

            @Override
            public boolean hasNext() {
                return sub.hasNext();
            }

            @Override
            public RowData next() {
                return nextWithPk().getValue();
            }
        };
    }

    @Override
    public RowIterator iterator() {
        return new RowIterator() {

            BigInteger currentIt = BigInteger.ZERO;
            Iterator<String[]> csvLines = recognizer.iterator();
            String[] headers = recognizer.getColumnNames();

            @Override
            public void setPointerPk(BigInteger pk) {
                if(pk.compareTo(currentIt) < 0) this.restart();
                if(pk.compareTo(currentIt) < 0) throw new DataBaseException("CSVTable->iterator->setPointerPk","PK informada é menor que a menor primary key alcançavel");
                while(pk.compareTo(currentIt)>0 && hasNext())
                    next();
            }

            @Override
            public void unlock() {
            }

            @Override
            public void restart() {
                currentIt = BigInteger.ZERO;
                csvLines = recognizer.iterator();
            }

            @Override
            public Map.Entry<BigInteger, RowData> nextWithPk() {
                String[] data = csvLines.next();
                if(data==null)return null;
                currentIt = currentIt.add(BigInteger.ONE);

                String[] columns = recognizer.getColumnNames();
                RowData rowData = new RowData();
                for (Column c:
                        getHeader().getPrototype()) {
                    if(c.getName().compareTo(CSVTable.pkName)==0){
                        rowData.setLong(c.getName(),currentIt.longValue(),c);
                        continue;
                    }
                    for (int x=0;x<columns.length;x++) {
                        String columnName = columns[x];
                        if(c.getName().compareToIgnoreCase(columnName) != 0)continue;
                        String val = data[x];
                        if(val == null || val.isEmpty() || val.strip().isEmpty()){
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

                return Map.entry(currentIt,rowData);
            }

            @Override
            public boolean hasNext() {
                return csvLines.hasNext();
            }

            @Override
            public RowData next() {
                Map.Entry<BigInteger, RowData> e = this.nextWithPk();
                if(e==null)return null;
                return e.getValue();
            }
        };
    }
}
