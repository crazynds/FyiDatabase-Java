package sgbd.source.table;

import engine.storage.common.AnonymousStorageRecord;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordStream;
import lib.BigKey;
import sgbd.prototype.column.Column;
import sgbd.prototype.RowData;
import sgbd.source.components.Header;
import sgbd.source.components.RowIterator;
import sgbd.source.index.Index;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class GenericTable extends Table {

    protected Index primaryIndex;
    protected AnonymousStorageRecord storage;

    public GenericTable(Header header) {
        super(header);
        this.storage = null;
    }

    @Override
    public void close() {
        this.storage.flush();
        this.storage.close();
        this.storage = null;
    }

    @Override
    public void clear() {
        if(this.storage==null)this.open();
        this.storage.restart();
        this.storage.flush();
        this.primaryIndex.clear();
    }

    @Override
    public void insert(RowData r) {
        translatorApi.validateRowData(r);
        Record record = translatorApi.convertToRecord(r);

        RowData old = this.primaryIndex.findByRef(r);
        if(old!=null){
            this.storage.update(old.getLong(Index.REFERENCE_COLUMN_NAME),record);
        }else{
            this.storage.write(record);
        }

        this.storage.flush();
    }
    @Override
    public void insert(List<RowData> r) {
        for (RowData row: r){
            this.insert(row);
        }
        this.storage.flush();
    }

    public RowData findByRef(RowData reference){
        RowData row = this.primaryIndex.findByRef(reference);
        RowIterator it = iterator(null,row);
        if(it.hasNext()){
            return it.next();
        }
        return null;
    }

    @Override

    public RowIterator iterator(List<String> columns) {
        return this.iterator(columns, null);
    }

    @Override
    public RowIterator iterator() {
        ArrayList<String> columns = new ArrayList<>();
        for (Column c:this.getTranslator()) {
            columns.add(c.getName());
        }
        return this.iterator(columns);
    }

    @Override
    protected RowIterator iterator(List<String> columns,RowData lowerBound) {
        return new RowIterator() {
            boolean started = false;
            RecordStream<Long> recordStream;
            Map<String, Column> metaInfo = translatorApi.generateMetaInfo(columns);

            private void start(){

                recordStream = (lowerBound==null)? storage.read(0L) :storage.read(lowerBound.getLong(Index.REFERENCE_COLUMN_NAME));
                recordStream.open();
                started=true;
            }

            @Override
            public void restart() {
                if(!started || recordStream==null)start();
                recordStream.reset();
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
            public RowData next() {
                if(!started)start();
                if(recordStream==null)return null;
                Record record = recordStream.next();
                if(record==null){
                    unlock();
                    return null;
                }
                RowData row = translatorApi.convertToRowData(record,metaInfo);
                row.setLong(Index.REFERENCE_COLUMN_NAME,recordStream.getKey());
                return row;
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

    public Index getPrimaryIndex(){
        return primaryIndex;
    }

}
