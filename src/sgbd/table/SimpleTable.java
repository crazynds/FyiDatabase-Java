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
import sgbd.table.components.RowIterator;

import java.math.BigInteger;
import java.util.*;

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
    public RowData find(BigInteger pk) {
        Record r =this.manager.read(pk);
        return translatorApi.convertToRowData(r);
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


    public RowIterator iterator(List<String> columns) {
        return new RowIterator() {
            boolean started = false;
            RecordStream recordStream;

            private void start(){
                recordStream = manager.sequencialRead();
                recordStream.open(false);
                started=true;
            }

            @Override
            public void setPointerPk(BigInteger pk) {
                if(!started)start();
                recordStream.setPointer(pk);
            }

            @Override
            public void restart() {
                if(!started)start();
                recordStream.reset();
            }

            @Override
            public Map.Entry<BigInteger,RowData> nextWithPk() {
                return new Map.Entry<BigInteger,RowData>() {
                    Record record = recordStream.next();
                    @Override
                    public BigInteger getKey() {
                        return translatorApi.getPrimaryKey(record);
                    }

                    @Override
                    public RowData getValue() {
                        return translatorApi.convertToRowData(record,columns);
                    }

                    @Override
                    public RowData setValue(RowData value) {
                        return null;
                    }
                };
            }

            @Override
            public boolean hasNext() {
                if(!started)start();
                boolean val = recordStream.hasNext();
                if(!val){
                    recordStream.close();
                    recordStream = null;
                }
                return val;
            }

            @Override
            public RowData next() {
                if(!started)start();
                Record record = recordStream.next();
                if(record==null)return null;
                return translatorApi.convertToRowData(record,columns);
            }

            @Override
            protected void finalize() throws Throwable {
                if(recordStream!=null)recordStream.close();
                super.finalize();
            }
        };
    }

    @Override
    public RowIterator iterator() {
        return new RowIterator() {
            boolean started = false;
            RecordStream recordStream;

            private void start(){
                recordStream = manager.sequencialRead();
                recordStream.open(false);
                started=true;
            }

            @Override
            public void setPointerPk(BigInteger pk) {
                if(!started)start();
                recordStream.setPointer(pk);
            }

            @Override
            public void restart() {
                if(!started)start();
                recordStream.reset();
            }

            @Override
            public Map.Entry<BigInteger,RowData> nextWithPk() {
                return new Map.Entry<BigInteger,RowData>() {
                    Record record = recordStream.next();
                    @Override
                    public BigInteger getKey() {
                        return translatorApi.getPrimaryKey(record);
                    }

                    @Override
                    public RowData getValue() {
                        return translatorApi.convertToRowData(record);
                    }

                    @Override
                    public RowData setValue(RowData value) {
                        return null;
                    }
                };
            }

            @Override
            public boolean hasNext() {
                if(!started)start();
                boolean val = recordStream.hasNext();
                if(!val){
                    recordStream.close();
                    recordStream = null;
                }
                return val;
            }

            @Override
            public RowData next() {
                if(!started)start();
                Record record = recordStream.next();
                if(record==null)return null;
                return translatorApi.convertToRowData(record);
            }

            @Override
            protected void finalize() throws Throwable {
                if(recordStream!=null)recordStream.close();
                super.finalize();
            }
        };
    }
}
