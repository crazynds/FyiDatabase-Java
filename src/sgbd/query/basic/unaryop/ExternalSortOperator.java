package sgbd.query.basic.unaryop;

import com.google.gson.Gson;
import engine.exceptions.DataBaseException;
import engine.file.FileManager;
import engine.util.Util;
import sgbd.prototype.Column;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.query.basic.Operator;
import sgbd.query.basic.Tuple;
import sgbd.query.basic.sourceop.TableScan;
import sgbd.table.SimpleTable;
import sgbd.table.Table;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

public class ExternalSortOperator extends UnaryOperation{

    protected SimpleTable externalSortedTable;
    protected File temp,dataFile;
    protected String source,column;

    protected TableScan scan;

    protected boolean revertBinarySortColumn;

    public ExternalSortOperator(Operator op, String source,String column, boolean revertBinarySortColumn){
        super(op);
        this.source = source;
        this.column = column;
        this.scan = null;
        this.externalSortedTable = null;
        this.revertBinarySortColumn=revertBinarySortColumn;
    }

    @Override
    public void open() {
        if(externalSortedTable!=null)return;

        operator.open();
        if(!operator.hasNext()){
            operator.close();
            return;
        }
        Prototype pt = new Prototype();
        Tuple t = operator.next();
        Column c = t.getContent(source).getMeta(column);
        pt.addColumn("__aux",8,Column.PRIMARY_KEY);
        pt.addColumn("sort",c.getSize(),
                (c.isShift8Size()?Column.SHIFT_8_SIZE_COLUMN:Column.NONE)|
                        Column.PRIMARY_KEY);
        pt.addColumn("reference",4, Column.NONE);
        pt.addColumn("size",4, Column.NONE);
        try {
            externalSortedTable = new SimpleTable("externalSorted", new FileManager(File.createTempFile("table", "sortOperation")), pt);
            dataFile = File.createTempFile("data","sortOperation");
            scan = new TableScan(externalSortedTable);
        }catch (IOException exception){
            throw new DataBaseException("ExternalSortOperator->newTempFile",exception.getMessage());
        }

        externalSortedTable.open();
        Gson gson = new Gson();
        ArrayList<RowData> inserts = new ArrayList<>();
        int val = 0;
        do{
            if(t==null)
                t = operator.next();
            String json = gson.toJson(t);
            RowData row = new RowData();
            row.setInt("__aux",val++);
            if(revertBinarySortColumn) {
                byte[] data = t.getContent(source).getData(column);
                row.setData("sort", Util.invertByteArray(data,data.length));
            }else
                row.setData("sort",t.getContent(source).getData(column));
            row.setInt("reference",0);
            row.setInt("size",0);
            inserts.add(row);
            t = null;
            if(inserts.size()>32)
                externalSortedTable.insert(inserts);
        }while (operator.hasNext());
        externalSortedTable.insert(inserts);

        operator.close();

        scan.open();
    }

    @Override
    public Tuple next() {
        if(scan!=null) {
            Tuple t = scan.next();
            if(revertBinarySortColumn) {
                byte[] data = t.getContent("externalSorted").getData("sort");
                t.getContent("externalSorted").setData("sort", Util.invertByteArray(data,data.length));
            }
            return t;
        }
        return null;
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
            externalSortedTable.close();
            dataFile.delete();
        }
    }
}
