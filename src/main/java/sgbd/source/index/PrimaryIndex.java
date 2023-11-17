package sgbd.source.index;

import engine.file.FileManager;
import engine.file.streams.ReadByteStream;
import engine.storage.sorted.BTreeStorageRecord;
import engine.util.Util;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInfoExtractor;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;
import lib.BigKey;
import sgbd.prototype.RowData;
import sgbd.prototype.column.Column;
import sgbd.prototype.metadata.Metadata;
import sgbd.source.Source;
import sgbd.source.components.Header;
import sgbd.source.components.RowIterator;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class PrimaryIndex extends Index<Long>{
    private BTreeStorageRecord storage;
    private int pkSize = this.src.getTranslator().getPrimaryKeySize();

    private static Header preparePrototype2(Header header,Source<Long> src){
        header = preparePrototype(header,src);
        //header.getPrototype().addColumn(new Column("ref", (short) src.getTranslator().getPrimaryKeySize(), Metadata.NONE));

        return header.setBool("non_unique",false);
    }
    public PrimaryIndex(Header header, Source<Long> src) {
        super(preparePrototype2(header,src), src);

    }

    @Override
    public void open() {
        if(storage==null)
            storage = new BTreeStorageRecord(new FileManager(header.getTablePath()), new RecordInfoExtractor() {
                @Override
                public BigKey getPrimaryKey(ByteBuffer rbs) {
                    BigKey key = new BigKey(rbs.slice(0,pkSize).array());
                    return key;
                }

                @Override
                public BigKey getPrimaryKey(ReadByteStream rbs) {
                    return this.getPrimaryKey(rbs.read(0,pkSize));
                }

                @Override
                public boolean isActiveRecord(ByteBuffer rbs) {
                    return true;
                }

                @Override
                public boolean isActiveRecord(ReadByteStream rbs) {
                    return true;
                }

                @Override
                public void setActiveRecord(Record r, boolean active) {

                }
            }, pkSize, 8);
    }

    @Override
    public void close() {
        if(storage!=null) {
            storage.flush();
            storage.close();
        }
        storage= null;
    }
    @Override
    protected void updateRef(BigKey key, Long reference) {
        storage.write(key, new GenericRecord(Util.convertLongToByteArray(reference,8)));
    }

    @Override
    public Long deleteRef(BigKey key) {
        RowData row = this.findByRef(key);
        if(row!=null) {
            storage.delete(key);
            return row.getLong("ref");
        }
        return null;
    }

    @Override
    public void clear() {
        storage.restart();
    }


    @Override
    public RowData findByRef(BigKey reference) {
        RecordStream<BigKey> stream = storage.read(reference);
        stream.open();
        try {
            if (stream.hasNext()) {
                Record r = stream.next();
                if(stream.getKey().compareTo(reference)==0) {
                    return src.findByRef(Util.convertByteArrayToNumber(r.getData()).longValue());
                }
            }
        }finally {
            stream.close();
        }
        return null;
    }

    @Override
    protected RowIterator<BigKey> iterator(List<String> columns, BigKey lowerbound) {
        return new RowIterator<BigKey>() {

            boolean started = false;
            RecordStream<BigKey> recordStream;
            Map<String, Column> metaInfo = translatorApi.generateMetaInfo(columns);

            private void start(){
                recordStream = storage.read(lowerbound);
                recordStream.open();
                started=true;
            }
            @Override
            public void restart() {
                if(!started || recordStream==null)start();
                recordStream.reset();
            }

            @Override
            public void unlock() {
                if(started)
                    recordStream.close();
            }

            @Override
            public BigKey getRefKey() {
                return recordStream.getKey();
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
                return translatorApi.convertBinaryToRowData(recordStream.getKey().getData(),metaInfo,false,true);
            }
        };
    }
}
