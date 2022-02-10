package sgbd.query.basic.unaryop;

import com.google.gson.Gson;
import engine.exceptions.DataBaseException;
import engine.file.FileManager;
import sgbd.prototype.Column;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.query.basic.Operator;
import sgbd.query.basic.Tuple;
import sgbd.table.SimpleTable;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;

public class ExternalSortOperator extends UnaryOperation{

    protected SimpleTable externalSortedTable;
    protected File temp;

    public ExternalSortOperator(Operator op, Comparator<Tuple> comparator ){
        super(op);
        Prototype pt = new Prototype();
        pt.addColumn("data",32000, Column.DINAMIC_COLUMN_SIZE|Column.SHIFT_8_SIZE_COLUMN);
        pt.addColumn("id",4, Column.PRIMARY_KEY);
        try {
            externalSortedTable = new SimpleTable("externalSorted", new FileManager(File.createTempFile("external", "Sort")), pt);
        }catch (IOException exception){
            throw new DataBaseException("ExternalSortOperator->newTempFile",exception.getMessage());
        }
    }

    @Override
    public void open() {
        externalSortedTable.open();
        Gson gson = new Gson();

        int val = 0;
        operator.open();
        while (operator.hasNext()){
            Tuple t = operator.next();
            String json = gson.toJson(t);
            RowData row = new RowData();
            row.setInt("id",val++);
            row.setString("data",json);
            externalSortedTable.insert(row);
        }
        operator.close();
    }

    @Override
    public Tuple next() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public void close() {
        externalSortedTable.close();
    }
}
