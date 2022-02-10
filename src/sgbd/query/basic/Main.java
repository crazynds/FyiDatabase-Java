package sgbd.query.basic;

import engine.info.Parameters;
import sgbd.info.Query;
import sgbd.prototype.Column;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.query.basic.binaryop.MergeJoin;
import sgbd.query.basic.sourceop.TableScan;
import sgbd.query.basic.unaryop.ExternalSortOperator;
import sgbd.query.basic.unaryop.FilterOperator;
import sgbd.table.SimpleTable;
import sgbd.table.Table;
import sgbd.util.Filter;

import java.util.Comparator;
import java.util.List;

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

        Operator op = new TableScan(users, List.of("id","nome","idade","email","idCidade"));
        Operator op2 = new FilterOperator(op, new Filter<Tuple>() {
            @Override
            public boolean match(Tuple entries) {
                return entries.getContent("users").getInt("idade")>18;
            }
        });

//        Operator join1 = new PKNestedLoopJoin(op2,new PKTableScan(cidades,null),"users","idCidade");

        Operator cidadeScan = new TableScan(cidades);
//        Operator join1 = new BlockNestedLoopJoin(cidadeScan,op2, new ComparableFilter<Tuple>() {
//            @Override
//            public boolean match(Tuple t1, Tuple t2) {
//                return t2.getContent("users").getInt("idCidade") ==
//                        t1.getContent("cidades").getInt("id");
//            }
//        });

        Operator join1 = new MergeJoin(op2,cidadeScan,"users","idCidade","cidades","id");

        Operator sorted = new ExternalSortOperator(join1, new Comparator<Tuple>() {
            @Override
            public int compare(Tuple o1, Tuple o2) {
                return o1.getContent("users").getInt("id").compareTo(
                        o2.getContent("users").getInt("id")
                );
            }
        });



        Operator executor = sorted;

        executor.open();
        while(executor.hasNext()){
            Tuple t = executor.next();
            String str = "";


            RowData row = t.getContent("users");

            str += "users.nome="+row.getString("nome")+" | ";
            str += "users.idade="+row.getInt("idade")+" | ";
            str += "users.email="+row.getString("email")+" | ";
            str += "users.idCidade="+row.getInt("idCidade")+" | ";

            row = t.getContent("cidades");
            str += "cidades.nome="+row.getString("nome")+" | ";
            str += "cidades.id="+row.getInt("id")+" | ";

            System.out.println(str);
        }
        //Fecha operador
        executor.close();


        //Fecha as tables, não serão mais acessadas
        users.close();
        cidades.close();

        System.out.println("Query performance: ");
        System.out.println("Buscas por chave primaria: "+ Query.PK_SEARCH);
        System.out.println("Tuplas sorteadas: "+Query.SORT_TUPLES);
        System.out.println("Comparações de FILTER: "+Query.FILTER);
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
