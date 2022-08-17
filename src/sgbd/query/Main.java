package sgbd.query;

import engine.info.Parameters;
import sgbd.info.Query;
import sgbd.prototype.Column;
import sgbd.prototype.ComplexRowData;
import sgbd.prototype.Prototype;
import sgbd.query.binaryop.BlockNestedLoopJoin;
import sgbd.query.sourceop.TableScan;
import sgbd.query.unaryop.ExternalSortOperator;
import sgbd.query.unaryop.FilterOperator;
import sgbd.table.SimpleTable;
import sgbd.table.Table;

import java.util.Arrays;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        Prototype p1 = new Prototype();
        p1.addColumn("id",4,Column.PRIMARY_KEY);
        p1.addColumn("nome",255,Column.DINAMIC_COLUMN_SIZE);
        p1.addColumn("anoNascimento",4,Column.NONE);
        p1.addColumn("email",120,Column.NONE);
        p1.addColumn("idade",4,Column.CAM_NULL_COLUMN);
        p1.addColumn("salario",4,Column.NONE);
        p1.addColumn("idCidade",4,Column.NONE);

        Prototype p2 = new Prototype();
        p2.addColumn("id",4, Column.PRIMARY_KEY);
        p2.addColumn("nome",255,Column.DINAMIC_COLUMN_SIZE);

        Table users = SimpleTable.openTable("users",p1);

        Table cidades = SimpleTable.openTable("cidades",p2);

        users.open();
        cidades.open();

        Operator selectSomeUsers = new TableScan(users, Arrays.asList("id","idade","nome", "idCidade"));

        Operator where = new FilterOperator(selectSomeUsers,(Tuple t)->{
            return t.getContent("users").getInt("idade") >=18;
        });

        Operator selectAllCidades = new TableScan(cidades);

        Operator join = new BlockNestedLoopJoin(where,selectAllCidades,(t1, t2) -> {
            return t1.getContent("users").getInt("idCidade") == t2.getContent("cidades").getInt("id");
        });

//        Operator as = new AsOperator(join, new Conversor() {
//            @Override
//            public Column metaInfo(Tuple t) {
//                return t.getContent("users").getMeta("nome");
//            }
//
//            @Override
//            public byte[] process(Tuple t) {
//                String formated = t.getContent("users").getString("nome")+" ("+t.getContent("users").getInt("idade")+")";
//                return formated.getBytes(StandardCharsets.UTF_8);
//            }
//        }, "formated");



        Operator sorted = new ExternalSortOperator(join,"cidades","nome",true);


        Operator executor=sorted;

        executor.open();
        while(executor.hasNext()){
            Tuple t = executor.next();
            String str = "";
            for (Map.Entry<String, ComplexRowData> row: t){
                for(Map.Entry<String,byte[]> data:row.getValue()) {
                    switch(data.getKey()){
                        case "idade":
                        case "anoNascimento":
                        case "id":
                        case "idCidade":
                        case "size":
                            str+=row.getKey()+"."+data.getKey()+"="+row.getValue().getInt(data.getKey());
                            break;
                        case "salario":
                            str+=row.getKey()+"."+data.getKey()+"="+row.getValue().getFloat(data.getKey());
                            break;
                        case "name":
                        case "nome":
                        default:
                            str+=row.getKey()+"."+data.getKey()+"="+row.getValue().getString(data.getKey());
                            break;
                    }
                    str+=" | ";
                }
            }
            System.out.println(str);
        }
        //Fecha operador
        executor.close();


        //Fecha as tables, não serão mais acessadas
        users.close();
        cidades.close();


        System.out.println("");
        System.out.println("");

        System.out.println("Query performance: ");
        System.out.println("Buscas por chave primaria: "+ Query.PK_SEARCH);
        System.out.println("Tuplas sorteadas: "+Query.SORT_TUPLES);
        System.out.println("Comparações de FILTER: "+Query.COMPARE_FILTER);
        System.out.println("Comparações de JOIN: "+Query.COMPARE_JOIN);

        System.out.println("Disk performance: ");
        System.out.println("Tempo seek escrita: "+(Parameters.IO_SEEK_WRITE_TIME)/1000000f+"ms");
        System.out.println("Tempo escrita: "+(Parameters.IO_WRITE_TIME)/1000000f+"ms");
        System.out.println("Tempo seek leitura: "+(Parameters.IO_SEEK_READ_TIME)/1000000f+"ms");
        System.out.println("Tempo leitura: "+(Parameters.IO_READ_TIME)/1000000f+"ms");
        System.out.println("Tempo de sync: "+(Parameters.IO_SYNC_TIME)/1000000f+"ms");
        System.out.println("Tempo total IO: "+(Parameters.IO_SYNC_TIME
            +Parameters.IO_SEEK_WRITE_TIME
            +Parameters.IO_READ_TIME
            +Parameters.IO_SEEK_READ_TIME
            +Parameters.IO_WRITE_TIME)/1000000f+"ms");
        System.out.println("Blocos carregados: "+Parameters.BLOCK_LOADED);
        System.out.println("Blocos salvos: "+Parameters.BLOCK_SAVED);
    }
}
