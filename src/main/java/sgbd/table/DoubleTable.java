package sgbd.table;

import engine.exceptions.DataBaseException;
import engine.file.FileManager;
import engine.util.Util;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.manager.FixedRecordManager;
import engine.virtualization.record.manager.RecordManager;
import sgbd.prototype.*;
import sgbd.table.components.Header;
import sgbd.table.components.RowIterator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DoubleTable extends Table{

    /*
        Primeira tabela vai armazenar as primary keys e seus respectivos indexes de posi��o
        Ja a segunda tabela vai armazenar os dados brutos sequencialmente na ordem que foram inseridos

        Economiza o rearanjo de dados pois somente a tabela index vai ter o re arranjo enquanto a tabela data sera sempre sequencial
     */
    protected RecordManager index;
    protected int maxSizeIndexRowData;
    protected TranslatorApi indexTranslator;

    protected RecordManager data;
    protected int maxSizeDataRowData;
    protected TranslatorApi dataTranslator;
    protected FileManager indexFile,dataFile;

    private Prototype startedPrototype;

    private long maxIdData=0;



    DoubleTable(Header header) {
        super(header);
        this.startedPrototype = header.getPrototype();

        header.set(Header.TABLE_TYPE,"DoubleTable");
        indexFile = new FileManager(header.get(Header.TABLE_NAME)+"-index.dat");
        dataFile = new FileManager(header.get(Header.TABLE_NAME)+"-data.dat");

        if(header.getBool("clear")){
            indexFile.clearFile();
            dataFile.clearFile();
            header.setBool("clear",false);
        }

        Prototype indexTable = new Prototype();
        Prototype dataTable = new Prototype();

        for (Column c:
                this.startedPrototype) {
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
        maxSizeDataRowData = dataTranslator.maxRecordSize();
    }

    @Override
    public void clear() {
        if(this.index==null || this.data==null)this.open();
        this.index.restart();
        this.data.restart();
        this.index.flush();
        this.data.flush();
    }

    @Override
    public void open() {
        if(index==null){
            index = new FixedRecordManager(indexFile,indexTranslator,maxSizeIndexRowData);
        }
        if(data==null){
            data = new FixedRecordManager(dataFile,dataTranslator,maxSizeDataRowData);
        }
    }

    @Override
    public void close() {
        this.index.flush();
        this.data.flush();
        this.index.close();
        this.data.close();
    }

    private void prepare(RowData indexRow,RowData dataRow){
        if(dataRow.getData("_ id _")==null)
            dataRow.setLong("_ id _",maxIdData++);
        this.dataTranslator.validateRowData(dataRow);
        if(dataRow.getData("_ ref _")==null)
            indexRow.setData("_ ref _",dataRow.getData("_ id _"));
        this.indexTranslator.validateRowData(indexRow);
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
        this.prepare(index,data);
        this.data.write(this.dataTranslator.convertToRecord(data));
        this.index.write(this.indexTranslator.convertToRecord(index));
        return this.translatorApi.getPrimaryKey(r);
    }

    @Override
    public void insert(List<RowData> list) {
        ArrayList<Record> indexList = new ArrayList<>();
        ArrayList<Record> dataList = new ArrayList<>();
        for (RowData r:
             list) {
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
            this.prepare(index,data);
            indexList.add(this.indexTranslator.convertToRecord(index));
            dataList.add(this.dataTranslator.convertToRecord(data));
        }
        this.data.write(dataList);
        this.index.write(indexList);
    }

    private ComplexRowData mountRowData(Record indexRecord,Map<String,Column> metaInfo){
        ComplexRowData rowComplex = new ComplexRowData();
        ComplexRowData row = this.indexTranslator.convertToRowData(indexRecord,metaInfo);

        BigInteger ref = null;

        for(Map.Entry<String,BData> data:row){
            if(data.getKey() == "_ ref _") {
                ref = data.getValue().getBigInteger();
            }else{
                if(metaInfo.containsKey(data.getKey()))
                    rowComplex.setBData(data.getKey(), data.getValue(), row.getMeta(data.getKey()));
            }
        }
        if(ref==null)throw new DataBaseException("DoubleTable->mountRowData","Referencia nao encontrada para a montagem do dado");
        Record dataRecord = this.data.read(ref);

        row = this.dataTranslator.convertToRowData(dataRecord,metaInfo);
        for(Map.Entry<String,BData> data:row){
            if(data.getKey()=="_ id _"){
                //ignore;
            }else{
                if(metaInfo.containsKey(data.getKey()))
                    rowComplex.setBData(data.getKey(), data.getValue(), row.getMeta(data.getKey()));
            }
        }
        return rowComplex;
    }

    @Override
    public ComplexRowData find(BigInteger pk) {
        Record record = index.read(pk);
        return this.mountRowData(record,this.translatorApi.generateMetaInfo(null));
    }

    @Override
    public ComplexRowData find(BigInteger pk, List<String> colunas) {
        Record record = index.read(pk);
        return this.mountRowData(record,this.translatorApi.generateMetaInfo(colunas));
    }

    @Override
    public RowData update(BigInteger pk, RowData r) {
        return null;
    }

    @Override
    public RowData delete(BigInteger pk) {
        Record recordIndex = index.read(pk);
        RowData row = this.indexTranslator.convertToRowData(recordIndex, this.indexTranslator.generateMetaInfo(Arrays.asList("_ ref _")));
        Record recordData = data.read(Util.convertByteArrayToNumber(row.getData("_ ref _")));
        ComplexRowData rowData = this.mountRowData(recordIndex,null);
        this.indexTranslator.setActiveRecord(recordIndex,false);
        this.indexTranslator.setActiveRecord(recordData,false);
        index.write(recordIndex);
        data.write(recordData);
        return rowData;
    }

    @Override
    public RowIterator iterator() {
        return this.iterator(null);
    }

    @Override
    public RowIterator iterator(List<String> columns) {
        return new RowIterator() {
            boolean started = false;
            RecordStream recordStream;
            Map<String, Column> metaInfo = translatorApi.generateMetaInfo(columns);

            private void start(){
                recordStream = index.sequencialRead();
                recordStream.open(false);
                started=true;
            }

            @Override
            public void setPointerPk(BigInteger pk) {
                if(!started)start();
                recordStream.setPointer(pk);
            }

            @Override
            public void restart() {
                if(!started)start();
                recordStream.reset();
            }

            @Override
            public Map.Entry<BigInteger, ComplexRowData> nextWithPk() {
                if(!started)start();
                if(recordStream==null)return null;
                Record record = recordStream.next();
                if(record==null)return null;
                return Map.entry(translatorApi.getPrimaryKey(record),mountRowData(record,metaInfo));
            }

            @Override
            public boolean hasNext() {
                if(!started)start();
                if(recordStream==null)return false;
                boolean val = recordStream.hasNext();
                if(!val){
                    recordStream.close();
                    recordStream = null;
                }
                return val;
            }

            @Override
            public ComplexRowData next() {
                if(!started)start();
                if(recordStream==null)return null;
                Record record = recordStream.next();
                if(record==null)return null;
                return mountRowData(record,metaInfo);
            }

            @Override
            protected void finalize() throws Throwable {
                if(recordStream!=null)recordStream.close();
                super.finalize();
            }
        };
    }
}
