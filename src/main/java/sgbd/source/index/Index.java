package sgbd.source.index;

import engine.util.Util;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.prototype.TranslatorApi;
import sgbd.prototype.metadata.Metadata;
import sgbd.source.Source;
import sgbd.source.components.Header;
import sgbd.source.components.RowIterator;

import java.math.BigInteger;
import java.util.List;

public abstract class Index<T> extends Source<BigInteger> {

    protected Source<T> src;
    protected boolean uniqueIndex;

    private static final String UNIQUE_COLUMN_NAME = "_ UNIQUE_CONT _";


    private static Header preparePrototype(Header header,Source src){
        if(!header.getBool("unique")){
            Prototype py = header.getPrototype();
            py.addColumn(UNIQUE_COLUMN_NAME,src.getTranslator().getPrimaryKeySize(), Metadata.PRIMARY_KEY|Metadata.IGNORE_COLUMN);

        }
        return header;
    }

    public Index(Header header,Source<T> src) {
        super(preparePrototype(header,src));

        uniqueIndex = header.getBool("unique");
        this.src = src;
    }

    protected void reindex(){
        this.clear();
        RowIterator<T> it = src.iterator();
        TranslatorApi srcTranslator = src.getTranslator();
        while(it.hasNext()){
            RowData row = it.next();
            if(!this.uniqueIndex) {
                BigInteger pk = srcTranslator.getPrimaryKey(row);
                row.setData(UNIQUE_COLUMN_NAME, Util.convertNumberToByteArray(pk,srcTranslator.getPrimaryKeySize()));
            }
            translatorApi.validateRowData(row);
            BigInteger key = translatorApi.getPrimaryKey(row);
            T ref = it.getRefKey();
            this.updateRef(key,ref);
        }
    }

    public abstract void updateRef(BigInteger key,T reference);
    public abstract void deleteRef(BigInteger key);

    @Override
    public RowData findByRef(BigInteger reference) {
        RowIterator<BigInteger> it = this.iterator(null,reference);
        if(it.hasNext()){
            RowData row = it.next();
            if(it.getRefKey().compareTo(reference) == 0)return row;
        }
        return null;
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

}
