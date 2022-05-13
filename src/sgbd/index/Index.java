package sgbd.index;

import engine.exceptions.DataBaseException;
import sgbd.prototype.Column;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.prototype.TranslatorApi;
import sgbd.table.Table;

import java.math.BigInteger;
import java.util.List;

public abstract class Index {

    private long UNIQUE_VALUE = 0;
    protected TranslatorApi translatorApi;
    protected String indexName;
    protected Table table;

    protected Table indexData;

    public Index(Table table, String indexName, List<String> columnToIndex,boolean unique)  {
        this.indexName=indexName;
        this.table=table;
        this.translatorApi = table.getTranslator();
        Prototype indexPt=new Prototype();
        for(Column col:table.getTranslator()){
            if(columnToIndex.contains(col.getName())){
                DataBaseException ex;
                if(col.camBeNull()){
                    String error="Coluna "+col.getName()+" não pode ser nula e ao mesmo tempo ser indexada!";
                    ex=new DataBaseException("Prototype->ValidateColumns",error);
                    ex.addValidation("NULL not in PRIMARY KEY");
                    throw ex;
                }
                if(col.isDinamicSize()){
                    String error="Coluna "+col.getName()+" não pode ser dinamica ao mesmo tempo que é usada como index!";
                    ex=new DataBaseException("Prototype->ValidateColumns",error);
                    ex.addValidation("DINAMIC not in PRIMARY KEY");
                    throw ex;
                }
                indexPt.addColumn("index-"+col.getName(),col.getSize(), col.getFlags()|Column.PRIMARY_KEY);
            }
        }
        if(!unique)
            indexPt.addColumn("uniqueValue",8,Column.PRIMARY_KEY);
        indexPt.addColumn("pk",table.getTranslator().getPrimaryKeySize(),Column.NONE);
        for (Column col:
                indexPt) {
            System.out.println(col.getName()+" - "+col.getFlags()+" - "+col.getFlags());
        }
    }

    public abstract void reindexTable();

    public abstract void addIndexRow(RowData row,BigInteger pk);
    public abstract void removeIndexRow(RowData row,BigInteger pk);

    public abstract BigInteger findIndexRow(RowData row);


    protected synchronized long getUniqueValue(){
        return UNIQUE_VALUE++;
    }
}
