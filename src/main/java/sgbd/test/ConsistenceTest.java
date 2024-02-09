package sgbd.test;

import engine.exceptions.DataBaseException;
import lib.BigKey;
import sgbd.prototype.RowData;
import sgbd.source.Source;
import sgbd.source.components.RowIterator;
import sgbd.source.table.Table;
import sgbd.util.global.Faker;


import java.util.*;

public class ConsistenceTest {

    private int seed;
    private Source source;
    private Map<String,String> columnTypes;


    public ConsistenceTest(Source t, Map<String,String> columnTypes, int seed){
        t.clear();
        this.source = t;
        this.columnTypes = columnTypes;
        this.seed = new Random(seed).nextInt();
    }


    public void generateRandomData(int qtdData){
        Random r = new Random(this.seed);
        Faker.replaceRandom(r);
        for(int x=0;x<qtdData;x++){
            RowData row = generetaRowData();
            this.source.insert(row);
        }
    }
    public void generateRandomDataBlock(int qtdData,int blockSize){
        Random r = new Random(this.seed);
        List<RowData> list = new ArrayList<>();
        Faker.replaceRandom(r);
        for(int x=0;x<qtdData;x++){
            RowData row = generetaRowData();
            list.add(row);
            if(list.size()>=blockSize) {
                this.source.insert(list);
                list.clear();
                //System.out.println("["+x+"/"+qtdData+"]");
            }
        }
        if(list.size()>0)
            this.source.insert(list);
        //System.out.println("["+qtdData+"/"+qtdData+"] Todas as entradas geradas");
    }
    public boolean checkConsistence(int qtdData){
        if(!(this.source instanceof Table))throw new DataBaseException("ConsistentTest->checkConsistence","Consistence Test only work with tables!");
        Table table = (Table) this.source;
        Random r = new Random(this.seed);
        Faker.replaceRandom(r);
        TreeMap<BigKey,RowData> invalidos = new TreeMap<>();
        boolean valid = true;
        long valids = 0;
        int ia,ib;
        String sa,sb;
        float fa,fb;
        String columnName="";
        for(int x=0;x<qtdData;x++){
            valid = true;
            RowData row = generetaRowData();
            BigKey pk = table.getPrimaryIndex().getTranslator().getPrimaryKey(row);
            RowData toCompare = table.getPrimaryIndex().findByRef(pk);
            if(invalidos.containsKey(pk)){
                System.out.println("Inconsistencia anterior justificada por substituição de id");
                invalidos.remove(pk);
                valids++;
            }
            if(toCompare==null){
                invalidos.put(pk,row);
                valid=false;
            }else for(Map.Entry<String,String> column:columnTypes.entrySet()) {
                columnName = column.getKey();
                String columnType = column.getValue();
                switch (columnType){
                    case "small":
                    case "int":
                        ia = row.getInt(columnName);
                        ib = toCompare.getInt(columnName);
                        if(ia!=ib)valid= false;
                        break;
                    case "string":
                        sa = row.getString(columnName);
                        sb =toCompare.getString(columnName);
                        if(sa.compareTo(sb)!=0)valid= false;
                        break;
                    case "float":
                        fa = row.getFloat(columnName);
                        fb =toCompare.getFloat(columnName);
                        if(fa!=fb)valid= false;
                        break;
                }
                if(!valid)break;
            }
            if(valid==false){
                invalidos.put(pk,row);
            }else valids++;
        }
        for (Map.Entry<BigKey,RowData> dat:
             invalidos.entrySet()) {
            RowData row = dat.getValue();
            BigKey pk = dat.getKey();
            RowData toCompare = table.getPrimaryIndex().findByRef(pk);
            valid = true;
            if(toCompare==null){
                valid = false;
                System.out.println("Inconsistencia encontrada (Item não encontrado)");
            }else for(Map.Entry<String,String> column:columnTypes.entrySet()) {
                columnName = column.getKey();
                String columnType = column.getValue();
                switch (columnType){
                    case "small":
                    case "int":
                        ia = row.getInt(columnName);
                        ib = toCompare.getInt(columnName);
                        if(ia!=ib)valid= false;
                        break;
                    case "string":
                        sa = row.getString(columnName);
                        sb =toCompare.getString(columnName);
                        if(sa.compareTo(sb)!=0)valid= false;
                        break;
                    case "float":
                        fa = row.getFloat(columnName);
                        fb =toCompare.getFloat(columnName);
                        if(fa!=fb)valid= false;
                        break;
                }
                if(!valid){
                    System.out.println("=================================");
                    printRowData(row);
                    printRowData(toCompare);
                    System.out.println("=================================");
                    System.out.println("Inconsistencia encontrada na coluna "+columnName);
                    break;
                }
            }
            if(!valid){
                printRowData(row);
                if(toCompare!=null)
                    printRowData(toCompare);
            }
        }
        System.out.println("["+valids+"/"+qtdData+"] "+((float)valids/qtdData)*100.0+"% de records válidos verificados!");

        return invalidos.isEmpty();
    }

    public void printAllData(){
        for (RowIterator<?> it = this.source.iterator(); it.hasNext(); ) {
            RowData row = it.next();
            printRowData(row);
        }
    }

    private void printRowData(RowData row){
        System.out.print("{ ");
        for(Map.Entry<String,String> column:columnTypes.entrySet()) {
            String columnName = column.getKey();
            String columnType = column.getValue();
            System.out.print(columnName+": ");
            switch (columnType){
                case "small":
                case "int":
                    System.out.print(row.getInt(columnName)+", ");
                    break;
                case "string":
                    System.out.print(row.getString(columnName)+", ");
                    break;
                case "float":
                    System.out.print(row.getFloat(columnName)+", ");
                    break;
            }
        }
        System.out.println(" }");
    }

    private RowData generetaRowData(){
        RowData row = new RowData();
        for(Map.Entry<String,String> column:columnTypes.entrySet()){
            String columnName = column.getKey();
            String columnType = column.getValue();

            switch (columnType){
                case "small":
                    row.setInt(columnName,Faker.integer(0,250));
                    break;
                case "int":
                    row.setInt(columnName,Faker.integer(0,Integer.MAX_VALUE));
                    break;
                case "string":
                    row.setString(columnName,Faker.firstName());
                    break;
                case "float":
                    row.setFloat(columnName,Faker.floatPoint(-2000,2000));
                    break;
            }
        }
        return row;
    }








}
