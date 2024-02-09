package sgbd.source.index;

import engine.exceptions.DataBaseException;
import engine.util.Util;
import lib.BigKey;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.prototype.TranslatorApi;
import sgbd.prototype.column.Column;
import sgbd.prototype.metadata.Metadata;
import sgbd.source.Source;
import sgbd.source.components.Header;
import sgbd.source.components.RowIterator;


import java.util.List;

public abstract class Index<T> extends Source<BigKey> {

    protected Source<T> src;
    protected boolean nonUniqueIndex;

    protected static final String UNIQUE_COLUMN_NAME = "@_ UNIQUE_CONT";


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
        header.setPrototype(py);
        return header;
    }

    public Index(Header header,Source<T> src) {
        super(header);

        nonUniqueIndex = header.getBool("non_unique");
        this.src = src;
    }

    public void reindex(){
        this.clear();
        RowIterator<T> it = src.iterator(translatorApi.getColumns().stream().map(column -> column.getName()).toList());
        TranslatorApi srcTranslator = src.getTranslator();
        while(it.hasNext()){
            RowData row = it.next();
            if(this.nonUniqueIndex) {
                BigKey pk = srcTranslator.getPrimaryKey(row);
                row.setData(UNIQUE_COLUMN_NAME, pk.getData());
            }
            T ref = it.getRefKey();
            this.update(row,ref);
        }
    }

    public BigKey insert(RowData r){
        return null;
    }
    public void insert(List<RowData> r){
        for (RowData row:
             r) {
            this.insert(row);
        }
    }


    public void update(RowData row,T reference){
        if(this.nonUniqueIndex) {
            BigKey pk = src.getTranslator().getPrimaryKey(row);
            row.setData(UNIQUE_COLUMN_NAME, pk.getData());
        }
        translatorApi.validateRowData(row);
        BigKey key = translatorApi.getPrimaryKey(row);
        this.updateRef(key,reference);

    }
    protected abstract void updateRef(BigKey key,T reference);
    public abstract T deleteRef(BigKey key);

    @Override
    public RowIterator<BigKey> iterator() {
        return this.iterator(null);
    }

    @Override
    public RowIterator<BigKey> iterator(List<String> columns) {
        return this.iterator(columns,null);
    }


    @Override
    public String getSourceName(){
        return header.get(Header.TABLE_NAME);
    }

}
