package sgbd.table;

import engine.virtualization.record.Record;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.manager.RecordManager;
import sgbd.prototype.Column;
import sgbd.prototype.ComplexRowData;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.table.components.Header;
import sgbd.table.components.RowIterator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class GenericTable extends Table{
    protected RecordManager manager;


    public GenericTable(Header header) {
        super(header);
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
        this.manager.flush();
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
        return translatorApi.convertToRowData(r,translatorApi.generateMetaInfo(colunas));
    }
    @Override
    public ComplexRowData find(BigInteger pk) {
        return this.find(pk,null);
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
        return translatorApi.convertToRowData(r,translatorApi.generateMetaInfo(null));
    }


    public RowIterator iterator(List<String> columns) {
        return new RowIterator() {
            boolean started = false;
            RecordStream recordStream;
            Map<String, Column> metaInfo = translatorApi.generateMetaInfo(columns);

            private void start(){
                recordStream = manager.sequencialRead();
                recordStream.open(false);
                started=true;
            }

            @Override
            public void setPointerPk(BigInteger pk) {
                if(!started || recordStream==null)start();
                recordStream.setPointer(pk);
            }

            @Override
            public void restart() {
                if(!started || recordStream==null)start();
                recordStream.reset();
            }

            @Override
            public Map.Entry<BigInteger, ComplexRowData> nextWithPk() {
                if(!started)start();
                if(recordStream==null)return null;
                Record record = recordStream.next();
                if(record==null){
                    unlock();
                    return null;
                }
                return Map.entry(translatorApi.getPrimaryKey(record),translatorApi.convertToRowData(record,metaInfo));
            }

            @Override
            public boolean hasNext() {
                if(!started)start();
                if(recordStream==null)return false;
                boolean val = recordStream.hasNext();
                if(!val)
                    unlock();
                return val;
            }

            @Override
            public ComplexRowData next() {
                if(!started)start();
                if(recordStream==null)return null;
                Record record = recordStream.next();
                if(record==null){
                    unlock();
                    return null;
                }
                return translatorApi.convertToRowData(record,metaInfo);
            }

            @Override
            public void unlock() {
                if (recordStream==null)
                    return;
                recordStream.close();
                recordStream = null;
            }
        };
    }

    @Override
    public RowIterator iterator() {
        ArrayList<String> columns = new ArrayList<>();
        for (Column c:this.getTranslator()) {
            columns.add(c.getName());
        }
        return this.iterator(columns);
    }
}
