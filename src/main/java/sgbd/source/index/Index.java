package sgbd.source.index;

import engine.exceptions.DataBaseException;
import engine.util.Util;
import lib.BigKey;
import sgbd.prototype.BData;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.prototype.TranslatorApi;
import sgbd.prototype.column.Column;
import sgbd.prototype.column.LongColumn;
import sgbd.prototype.metadata.Metadata;
import sgbd.prototype.query.fields.Field;
import sgbd.prototype.query.fields.IntegerField;
import sgbd.prototype.query.fields.LongField;
import sgbd.source.Source;
import sgbd.source.components.Header;
import sgbd.source.components.RowIterator;
import sgbd.source.table.Table;
import sgbd.util.global.UtilConversor;


import java.util.List;

public abstract class Index extends Table {

    protected Source src;
    protected boolean nonUniqueIndex,primary_index;
    protected static final String UNIQUE_COLUMN_NAME = "@_ UNIQUE_CONT";
    public static final String REFERENCE_COLUMN_NAME = "@_ REFERENCE _";


    protected static Header preparePrototype(Header header,Source src){
        Prototype py = new Prototype();
        if(header.getBool("non_unique") &&
                header.getPrototype().getColumns().stream()
                        .anyMatch(column -> column.getName().compareTo(UNIQUE_COLUMN_NAME)==0) == false){
            py.addColumn(UNIQUE_COLUMN_NAME,src.getTranslator().getPrimaryKeySize(), Metadata.PRIMARY_KEY|Metadata.IGNORE_COLUMN);
            throw new DataBaseException("Index->preparePrototype","Ainda não está funcionando os indices secundários não unicos.");
        }
        for(Column c:header.getPrototype()){
            if(c.isPrimaryKey()){
                py.addColumn(c);
            }
        }
        if(header.getBool("primary_index")){
            py.addColumn(new LongColumn(REFERENCE_COLUMN_NAME));
        }
        header.setPrototype(py);
        return header;
    }

    public Index(Header header, Source src) {
        super(header);

        this.src = src;
        primary_index = header.getBool("primary_index");
        nonUniqueIndex = header.getBool("non_unique");
    }

    public void reindex(){
        this.clear();
        RowIterator it = src.iterator(translatorApi.getColumns().stream().map(column -> column.getName()).toList());
        while(it.hasNext()){
            RowData row = it.next();
            this.insert(row);
        }
    }

    protected abstract void insertPair(BigKey key, BigKey reference);
    public void insert(RowData r){
        BigKey ref = (this.primary_index) ? r.getBigKey(REFERENCE_COLUMN_NAME) : src.getTranslator().getPrimaryKey(r);
        if(this.nonUniqueIndex) {
            r.setData(UNIQUE_COLUMN_NAME, src.getTranslator().getPrimaryKey(r).getData());
        }
        translatorApi.validateRowData(r);
        BigKey key = translatorApi.getPrimaryKey(r);
        this.insertPair(key,ref);
    }


    public void insert(List<RowData> r){
        for (RowData row:
             r) {
            this.insert(row);
        }
    }

    protected abstract RowIterator idxIterator(List<String> columns, BigKey lowerbound);


    public RowData findByRef(RowData reference){
        RowIterator it = iterator(null,reference);
        if(it.hasNext()){
            BigKey key1 = getTranslator().getPrimaryKey(reference);

            RowData row = it.next();
            if(row==null)return null;
            BigKey key2 = getTranslator().getPrimaryKey(row);
            return (key1.compareTo(key2)==0)?row:null;
        }

        return null;
    }

    @Override
    protected RowIterator iterator(List<String> columns, RowData lowerbound){
        return this.idxIterator(columns,lowerbound==null ? null : translatorApi.getPrimaryKey(lowerbound));
    }
    @Override
    public RowIterator iterator() {
        return this.iterator(null);
    }

    @Override
    public RowIterator iterator(List<String> columns) {
        return this.iterator(columns,null);
    }


    @Override
    public String getSourceName(){
        return header.get(Header.TABLE_NAME);
    }

    public int sizeOfKey(){
        return getTranslator().getPrimaryKeySize();
    }

    public int sizeOfBody(){
        return (this.primary_index) ? 8 : src.getTranslator().getPrimaryKeySize();
    }

    protected RowData generateRowData(BigKey key, BigKey body){
        RowData row = getTranslator().primaryKeyToRowData(key);

        if(this.primary_index){
            row.setLong(REFERENCE_COLUMN_NAME, UtilConversor.byteArrayToLong(body.getData()));
        }else{
            row = src.getTranslator().primaryKeyToRowData(body,row);
        }

        return row;
    }

}
