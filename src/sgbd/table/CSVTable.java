package sgbd.table;

import engine.exceptions.DataBaseException;
import sgbd.prototype.Column;
import sgbd.prototype.ComplexRowData;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.table.components.Header;
import sgbd.table.components.RowIterator;
import sgbd.util.Util;

import java.io.*;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class CSVTable extends Table{
    File csvFile;

    public CSVTable(Header header) throws FileNotFoundException {
        super(header);
        this.csvFile = new File(header.getTablePath());
    }

    @Override
    public void clear() {
        throw new DataBaseException("CSVTable","This type of table (CSVTable) is not writable");
    }

    @Override
    public void open() {
        if(!csvFile.exists() || !csvFile.isFile() || !csvFile.canRead())
            throw new RuntimeException("CSV não é acessivel!");
    }

    @Override
    public void close() {
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
    public ComplexRowData find(BigInteger pk) {
        for (RowIterator it = this.iterator(); it.hasNext(); ) {
            Map.Entry<BigInteger, ComplexRowData> row = it.nextWithPk();
            if(row.getKey().compareTo(pk) == 0)return row.getValue();
        }
        return null;
    }

    @Override
    public ComplexRowData find(BigInteger pk, List<String> columns) {
        for (RowIterator it = this.iterator(columns); it.hasNext(); ) {
            Map.Entry<BigInteger, ComplexRowData> row = it.nextWithPk();
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
            @Override
            public void setPointerPk(BigInteger pk) {
                sub.setPointerPk(pk);
            }

            @Override
            public void restart() {
                sub.restart();
            }

            @Override
            public Map.Entry<BigInteger, ComplexRowData> nextWithPk() {
                Map.Entry<BigInteger, ComplexRowData> comp = sub.nextWithPk();
                if(comp==null)
                    return null;
                return Map.entry(comp.getKey(),translatorApi.convertToRowData(translatorApi.convertToRecord(comp.getValue()),columns));
            }

            @Override
            public boolean hasNext() {
                return sub.hasNext();
            }

            @Override
            public ComplexRowData next() {
                return nextWithPk().getValue();
            }
        };
    }

    @Override
    public RowIterator iterator() {

        return new RowIterator() {

            BufferedReader br;
            Stream<String> lines;
            Iterator<String> iterator;

            {
                try {
                    br = new BufferedReader(new FileReader(csvFile));
                    lines = br.lines();
                    iterator = lines.iterator();
                    iterator.next();    // Ignore header
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void setPointerPk(BigInteger pk) {
                throw new DataBaseException("CSVTable","This type of table (CSVTable) not support search by PrimaryKey");
            }

            @Override
            public void restart() {
                try {
                    iterator = null;
                    lines.close();
                    br.close();
                    br = new BufferedReader(new FileReader(csvFile));
                    lines = br.lines();
                    iterator = lines.iterator();
                    iterator.next();    // Ignore header
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Map.Entry<BigInteger, ComplexRowData> nextWithPk() {
                ComplexRowData row = new ComplexRowData();
                String line = iterator.next();
                String[] columns = line.split(",");
                Prototype p = getHeader().getPrototype();
                for (int x=0;x<columns.length && x<columns.length;x++) {
                    Column c = p.getColumn(x);
                    if(c==null)break;
                    String s = columns[x];
                    try {
                        switch (Util.typeOfColumn(c)) {
                            case "float":
                                row.setFloat(c.getName(), Float.parseFloat(s), c);
                                break;
                            case "double":
                                row.setDouble(c.getName(), Double.parseDouble(s), c);
                                break;
                            case "int":
                                row.setInt(c.getName(), Integer.parseInt(s), c);
                                break;
                            case "string":
                            default:
                                row.setString(c.getName(), s, c);
                                break;
                        }
                    }catch (NumberFormatException e){
                        row.setString(c.getName(),s,new Column(c.getName(), (short) s.length(),Column.STRING));
                    };
                }
                BigInteger pk = translatorApi.getPrimaryKey(row);
                return Map.entry(pk,row);
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public ComplexRowData next() {
                return nextWithPk().getValue();
            }


            @Override
            protected void finalize() throws Throwable {
                iterator = null;
                lines.close();
                br.close();
                super.finalize();
            }
        };
    }
}
