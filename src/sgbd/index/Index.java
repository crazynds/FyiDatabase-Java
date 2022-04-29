package sgbd.index;

import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.prototype.TranslatorApi;
import sgbd.table.Table;

import java.math.BigInteger;

public abstract class Index {

    protected TranslatorApi translatorApi;
    protected String indexName;
    protected Table table;

    public Index(Table table,String indexName)  {
        this.indexName=indexName;
        this.table=table;
        this.translatorApi = table.getTranslator();
    }

    public abstract void reindexTable(Table t);

    public abstract void setIndexRow(RowData row,BigInteger pk);
    public abstract void removeIndexRow(RowData row,BigInteger pk);

    public abstract BigInteger findIndexRow(RowData row);





}
