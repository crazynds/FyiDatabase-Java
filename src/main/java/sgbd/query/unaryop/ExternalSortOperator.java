package sgbd.query.unaryop;

import com.google.gson.Gson;
import engine.exceptions.DataBaseException;
import engine.util.Util;
import sgbd.prototype.column.Column;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;
import sgbd.query.sourceop.TableScan;
import sgbd.table.Table;
import sgbd.table.components.Header;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Deprecated
public class ExternalSortOperator extends UnaryOperator {

    private static final String TABLE_NAME = "__externalSortedTable__";

    protected Table externalSortedTable;
    protected File temp,dataFile;
    protected String source,column;

    protected TableScan scan;

    protected boolean revertBinarySortColumn;

    protected RandomAccessFile fileWriter;
    private Gson gson;

    public ExternalSortOperator(Operator op, String source,String column, boolean revertBinarySortColumn){
        super(op);
        this.source = source;
        this.column = column;
        this.scan = null;
        this.externalSortedTable = null;
        this.revertBinarySortColumn=revertBinarySortColumn;
        gson = new Gson();
    }

    @Override
    public void open() {
        if(externalSortedTable!=null){
            scan.open();
            return;
        }
        super.open();

        if(!operator.hasNext()){
            operator.close();
            return;
        }

        Prototype pt = new Prototype();
        Tuple t = operator.next();
        Column c = t.getContent(source).getMetadata(column);
        pt.addColumn("__aux",8,Column.PRIMARY_KEY);
        pt.addColumn("sort",c.getSize(),
                (c.isShift8Size()?Column.LSHIFT_8_SIZE_COLUMN:Column.NONE)|
                        Column.PRIMARY_KEY);
        pt.addColumn("reference",8, Column.NONE);
        pt.addColumn("size",4, Column.NONE);

        try {
            Header header = new Header(pt,TABLE_NAME);
            externalSortedTable = Table.openTable(header,true);
            scan = new TableScan(externalSortedTable, List.of("reference","size"));

            dataFile = File.createTempFile("data","sortOperation");
            fileWriter = new RandomAccessFile(this.dataFile,"rw");

        }catch (IOException exception){
            throw new DataBaseException("ExternalSortOperator->newTempFile",exception.getMessage());
        }finally {
            operator.close();
        }


        externalSortedTable.open();
        operator.open();
        ArrayList<RowData> inserts = new ArrayList<>();

        long unique_val = 0;
        long startPos = 0;
        do{
            t = operator.next();
            String json = gson.toJson(t);
            RowData row = new RowData();
            row.setLong("__aux",unique_val++);
            if(revertBinarySortColumn) {
                byte[] data = t.getContent(source).getData(column).clone();
                row.setData("sort", Util.invertByteArray(data,data.length));
            }else
                row.setData("sort",t.getContent(source).getData(column));
            byte[] data = json.getBytes(StandardCharsets.UTF_8);
            try {
                fileWriter.seek(startPos);
                fileWriter.write(data,0,data.length);
            }catch (Exception e){
                scan.close();
                externalSortedTable.close();
                throw new DataBaseException("ExternalSortOperator->open",e.getMessage());
            }
            row.setLong("reference",startPos);
            row.setInt("size",data.length);
            startPos+=data.length;
            inserts.add(row);
            t = null;
            if(inserts.size()>32)
                externalSortedTable.insert(inserts);
        }while (operator.hasNext());
        try {
            fileWriter.getFD().sync();
        }catch (Exception e){
            throw new DataBaseException("ExternalSortOperator->open",e.getMessage());
        }
        externalSortedTable.insert(inserts);

        operator.close();

        scan.open();
    }

    @Override
    public Tuple next() {
        if(scan==null)
            return null;

        Tuple t = scan.next();
        RowData row = t.getContent(TABLE_NAME);
        long pos = row.getLong("reference");
        int size = row.getInt("size");

        byte[] data = new byte[size];
        try {
            fileWriter.seek(pos);
            fileWriter.read(data,0,data.length);
        } catch (IOException e) {
            throw new DataBaseException("ExternalSortOperator->next",e.getMessage());
        }
        String json = new String(data,StandardCharsets.UTF_8);

        Tuple tuple = (Tuple)gson.fromJson(json,Tuple.class);
        return tuple;
    }

    @Override
    public boolean hasNext() {
        if(scan==null)return false;
        return scan.hasNext();
    }

    @Override
    public void close() {
        if(scan!=null) {
            scan.close();
        }else{
            super.close();
        }
    }

    @Override
    public void freeResources() {
        super.freeResources();
        if(scan!=null) {
            try {
                fileWriter.close();
            } catch (IOException e) {
            }
            scan.freeResources();
            externalSortedTable.close();
            dataFile.delete();
            String path = externalSortedTable.getHeader().get(Header.FILE_PATH);
            if (path != null) {
                (new File(path)).delete();
            }
            externalSortedTable = null;
        }
    }
}
