package sgbd.table;

import engine.file.FileManager;
import engine.virtualization.record.Record;
import engine.virtualization.record.manager.FixedRecordManager;
import engine.virtualization.record.manager.RecordManager;
import sgbd.prototype.*;
import sgbd.table.components.RowIterator;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class DoubleTable extends Table{

    /*
        Primeira tabela vai armazenar as primary keys e seus respectivos indexes de posição
        Ja a segunda tabela vai armazenar os dados brutos sequencialmente na ordem que foram inseridos

        Economiza o rearanjo de dados pois somente a tabela index vai ter o re arranjo enquanto a tabela data sera sempre sequencial
     */
    protected RecordManager index;
    protected int maxSizeIndexRowData;
    protected TranslatorApi indexTranslator;

    protected RecordManager data;
    protected int maxSizeDataRowData;
    protected TranslatorApi dataTranslator;

    private Prototype startedPrototype;

    private long maxIdData=0;


    public DoubleTable(String tableName, Prototype pt) {
        super(tableName, pt);
        this.startedPrototype = pt;
        Prototype indexTable = new Prototype();
        Prototype dataTable = new Prototype();

        for (Column c:
             pt) {
            if(c.isPrimaryKey()){
                indexTable.addColumn(c);
            }else{
                dataTable.addColumn(c);
            }
        }
        indexTable.addColumn("_ ref _",8,Column.SIGNED_INTEGER_COLUMN);
        dataTable.addColumn("_ id _",8,Column.PRIMARY_KEY|Column.SIGNED_INTEGER_COLUMN);
        indexTranslator = indexTable.validateColumns();
        dataTranslator = dataTable.validateColumns();
        maxSizeIndexRowData = indexTranslator.maxRecordSize();
        maxSizeIndexRowData = dataTranslator.maxRecordSize();


    }

    @Override
    public void open() {
        if(index==null){
            index = new FixedRecordManager(new FileManager(tableName+"-index.dat"),indexTranslator,maxSizeIndexRowData);
        }
        if(data==null){
            data = new FixedRecordManager(new FileManager(tableName+"-index.dat"),dataTranslator,maxSizeDataRowData);
        }
    }

    @Override
    public void close() {
        this.index.flush();
        this.data.flush();
        this.index.close();
        this.data.close();
    }

    private void insert(RowData indexRow,RowData dataRow){
        dataRow.setLong("_ id _",maxIdData++);
        this.dataTranslator.validateRowData(dataRow);
        this.data.write(this.dataTranslator.convertToRecord(dataRow));
        indexRow.setData("_ ref _",dataRow.getData("_ id _"));

        this.indexTranslator.validateRowData(indexRow);
        this.index.write(this.indexTranslator.convertToRecord(indexRow));
    }

    @Override
    public BigInteger insert(RowData r) {
        this.translatorApi.validateRowData(r);

        RowData index = new RowData();
        RowData data = new RowData();
        for (Column c:
                startedPrototype) {
            if (c.isPrimaryKey()) {
                index.setData(c.getName(),r.getData(c.getName()));
            } else {
                data.setData(c.getName(),r.getData(c.getName()));
            }
        }
        this.insert(index,data);
        return this.translatorApi.getPrimaryKey(r);
    }

    @Override
    public void insert(List<RowData> list) {
        for (RowData r:
             list) {
            this.insert(r);
        }
    }

    private ComplexRowData mountRowData(Record indexRecord){
        ComplexRowData rowComplex = new ComplexRowData();
        ComplexRowData row = this.indexTranslator.convertToRowData(indexRecord);

        for(Map.Entry<String,byte[]> data:row){
            if(data.getKey() == "_ ref _") {

            }else{
                rowComplex.setData(data.getKey(), data.getValue(), row.getMeta(data.getKey()));
            }
        }


        return null;
    }

    @Override
    public ComplexRowData find(BigInteger pk) {
        Record record = index.read(pk);
        return this.mountRowData(record);
    }

    @Override
    public ComplexRowData find(BigInteger pk, List<String> colunas) {
        Record record = index.read(pk);
        return this.mountRowData(record);
    }

    @Override
    public RowData update(BigInteger pk, RowData r) {
        return null;
    }

    @Override
    public RowData delete(BigInteger pk) {
        return null;
    }

    @Override
    public RowIterator iterator(List<String> columns) {
        return this.iterator();
    }

    @Override
    public RowIterator iterator() {
        return new RowIterator() {
            @Override
            public void setPointerPk(BigInteger pk) {

            }

            @Override
            public void restart() {

            }

            @Override
            public Map.Entry<BigInteger, ComplexRowData> nextWithPk() {
                return null;
            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public ComplexRowData next() {
                return null;
            }
        };
    }
}
