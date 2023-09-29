package sgbd.source.index;

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
    protected boolean uniqueIndex;

    private static final String UNIQUE_COLUMN_NAME = "@_ UNIQUE_CONT";


    private static Header preparePrototype(Header header,Source src){
        Prototype py = new Prototype();
        if(!header.getBool("unique")){
            py.addColumn(UNIQUE_COLUMN_NAME,src.getTranslator().getPrimaryKeySize(), Metadata.PRIMARY_KEY|Metadata.IGNORE_COLUMN);
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
        super(preparePrototype(header,src));

        uniqueIndex = header.getBool("unique");
        this.src = src;
    }

    protected void reindex(){
        this.clear();
        RowIterator<T> it = src.iterator(translatorApi.getColumns().stream().map(column -> column.getName()).toList());
        TranslatorApi srcTranslator = src.getTranslator();
        while(it.hasNext()){
            RowData row = it.next();
            if(!this.uniqueIndex) {
                BigKey pk = srcTranslator.getPrimaryKey(row);
                row.setData(UNIQUE_COLUMN_NAME, pk.getData());
            }
            T ref = it.getRefKey();
            this.update(row,ref);
        }
    }
    public void update(RowData row,T reference){
        if(!this.uniqueIndex) {
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
    public RowData findByRef(BigKey reference) {
        RowIterator<BigKey> it = this.iterator(null,reference);
        if(it.hasNext()){
            RowData row = it.next();
            if(it.getRefKey().compareTo(reference) == 0)return row;
        }
        return null;
    }

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
