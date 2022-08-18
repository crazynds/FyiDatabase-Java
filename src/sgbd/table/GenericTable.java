package sgbd.table;

import engine.virtualization.record.Record;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.manager.RecordManager;
import sgbd.prototype.ComplexRowData;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.table.components.RowIterator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class GenericTable extends Table{
    protected RecordManager manager;


    public GenericTable(String tableName, Prototype pt) {
        super(tableName,pt);
    }

    @Override
    public void close() {
        this.manager.flush();
        this.manager.close();
    }

    @Override
    public void clear() {
        if(this.manager==null)this.open();
        this.manager.restart();
        this.manager.flush();
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
        this.manager.flush();
    }

    @Override
    public ComplexRowData find(BigInteger pk, List<String> colunas) {
        Record r =this.manager.read(pk);
        return translatorApi.convertToRowData(r,colunas);
    }
    @Override
    public ComplexRowData find(BigInteger pk) {
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
        this.manager.flush();
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
            public Map.Entry<BigInteger, ComplexRowData> nextWithPk() {
                if(!started)start();
                if(recordStream==null)return null;
                Record record = recordStream.next();
                if(record==null)return null;
                return Map.entry(translatorApi.getPrimaryKey(record),translatorApi.convertToRowData(record,columns));
            }

            @Override
            public boolean hasNext() {
                if(!started)start();
                if(recordStream==null)return false;
                boolean val = recordStream.hasNext();
                if(!val){
                    recordStream.close();
                    recordStream = null;
                }
                return val;
            }

            @Override
            public ComplexRowData next() {
                if(!started)start();
                if(recordStream==null)return null;
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
            public Map.Entry<BigInteger,ComplexRowData> nextWithPk() {
                if(recordStream==null)return null;
                return new Map.Entry<BigInteger,ComplexRowData>() {
                    Record record = recordStream.next();
                    @Override
                    public BigInteger getKey() {
                        return translatorApi.getPrimaryKey(record);
                    }

                    @Override
                    public ComplexRowData getValue() {
                        return translatorApi.convertToRowData(record);
                    }

                    @Override
                    public ComplexRowData setValue(ComplexRowData value) {
                        return null;
                    }
                };
            }

            @Override
            public boolean hasNext() {
                if(!started)start();
                if(recordStream==null)return false;
                boolean val = recordStream.hasNext();
                if(!val){
                    recordStream.close();
                    recordStream = null;
                }
                return val;
            }

            @Override
            public ComplexRowData next() {
                if(!started)start();
                if(recordStream==null)return null;
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
