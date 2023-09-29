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
    }

    @Override
    public BigKey insert(RowData r) {
        translatorApi.validateRowData(r);
        BigKey pk = translatorApi.getPrimaryKey(r);
        Record record = translatorApi.convertToRecord(r);

        Long ref = this.primaryIndex.deleteRef(pk);
        if(ref!=null){
            this.storage.update(ref,record);
        }else{
            this.storage.write(record);
        }
        this.storage.flush();
        return pk;
    }
    @Override
    public void insert(List<RowData> r) {
        ArrayList<Record> list = new ArrayList<>();
        for (RowData row: r){
            translatorApi.validateRowData(row);
            Record record = translatorApi.convertToRecord(row);

            Long ref = this.primaryIndex.deleteRef(translatorApi.getPrimaryKey(record));
            if(ref!=null){  // Se já existia a entrada com aquela chave na tabela, então sobrescreve a entrada antiga
                this.storage.update(ref,record);
            }else{
                list.add(record);
            }
        }
        this.storage.write(list);
        this.storage.flush();
    }

    @Override

    public RowIterator iterator(List<String> columns) {
        return this.iterator(columns, 0L);
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
    protected RowIterator iterator(List<String> columns,Long lowerBound) {
        return new RowIterator() {
            boolean started = false;
            RecordStream<Long> recordStream;
            Map<String, Column> metaInfo = translatorApi.generateMetaInfo(columns);

            private void start(){
                recordStream = storage.read(lowerBound);
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
                return translatorApi.convertToRowData(record,metaInfo);
            }

            @Override
            public void unlock() {
                if (recordStream==null)
                    return;
                recordStream.close();
                recordStream = null;
            }

            @Override
            public Long getRefKey() {
                return recordStream.getKey();
            }
        };
    }

}
