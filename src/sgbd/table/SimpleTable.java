package sgbd.table;

import engine.file.FileManager;
import engine.file.buffers.OptimizedFIFOBlockBuffer;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;
import engine.virtualization.record.manager.FixedRecordManager;
import engine.virtualization.record.manager.RecordManager;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class SimpleTable extends Table {

    private FileManager fm;
    private RecordManager manager;

    private int maxRecordSize;

    public SimpleTable(String tableName, FileManager fm, Prototype pt) {
        super(tableName,pt);
        this.fm = fm;
        maxRecordSize = this.translatorApi.maxRecordSize();
    }

    public static Table openTable(String name,Prototype p){
        return SimpleTable.openTable(name,p,false);
    }
    public static Table openTable(String name,Prototype p, boolean clear){
        FileManager fm = new FileManager(name+".dat", new OptimizedFIFOBlockBuffer(4));
        if(clear)fm.clearFile();
        return new SimpleTable(name,fm,p);
    }

    @Override
    public void open() {
        if(manager==null)
            this.manager = new FixedRecordManager(this.fm,this.translatorApi,this.translatorApi.maxRecordSize());
        //Restart is temporary
        //this.manager.restart();
    }

    @Override
    public void close() {
        this.manager.flush();
        this.manager.close();
    }

    @Override
    public BigInteger insert(RowData r) {
        translatorApi.validateRowData(r);
        BigInteger pk = translatorApi.getPrimaryKey(r);
        Record record = translatorApi.convertToRecord(r);
        this.manager.write(record);
        return pk;
    }
    @Override
    public void insert(List<RowData> r) {
        ArrayList<Record> list = new ArrayList<>();
        for (RowData row: r){
            translatorApi.validateRowData(row);
            Record record = translatorApi.convertToRecord(row);
            list.add(record);
        }
        this.manager.write(list);
    }

    @Override
    public RowData find(BigInteger pk, List<String> colunas) {
        Record r =this.manager.read(pk);
        return translatorApi.convertToRowData(r,colunas);
    }

    @Override
    public RowData update(BigInteger pk,RowData r) {
        return null;
    }

    @Override
    public RowData delete(BigInteger pk) {
        Record r = this.manager.read(pk);
        translatorApi.setActiveRecord(r,false);
        this.manager.write(r);
        return translatorApi.convertToRowData(r);
    }


    public Iterator<RowData> iterator(List<String> columns) {
        return new Iterator<RowData>() {
            RecordStream recordStream;
            boolean started = false;

            @Override
            public boolean hasNext() {
                if(!started){
                    recordStream = manager.sequencialRead();
                    recordStream.open(false);
                    started=true;
                }
                if(recordStream==null)return false;
                boolean val = recordStream.hasNext();
                if(!val){
                    recordStream.close();
                    recordStream = null;
                }
                return val;
            }

            @Override
            public RowData next() {
                if(!started)hasNext();
                if(recordStream==null)return null;
                Record record = recordStream.next();
                if(record==null)return null;
                return translatorApi.convertToRowData(record,columns);
            }

        };
    }

    @Override
    public Iterator<RowData> iterator() {
        return new Iterator<RowData>() {
            RecordStream recordStream;
            boolean started = false;

            @Override
            public boolean hasNext() {
                if(!started){
                    recordStream = manager.sequencialRead();
                    recordStream.open(false);
                    started=true;
                }
                if(recordStream==null)return false;
                boolean val = recordStream.hasNext();
                if(!val){
                    recordStream.close();
                    recordStream = null;
                }
                return val;
            }

            @Override
            public RowData next() {
                if(!started)hasNext();
                if(recordStream==null)return null;
                Record record = recordStream.next();
                if(record==null)return null;
                return translatorApi.convertToRowData(record);
            }

        };
    }
}
