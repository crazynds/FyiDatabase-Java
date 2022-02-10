package sgbd.query.basic;

import sgbd.prototype.Column;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.query.basic.binaryop.NestedLoopJoin;
import sgbd.query.basic.sourceop.TableScan;
import sgbd.query.basic.unaryop.FilterOperator;
import sgbd.query.basic.unaryop.SortOperator;
import sgbd.table.SimpleTable;
import sgbd.table.Table;
import sgbd.util.ComparableFilter;
import sgbd.util.Filter;

import java.util.Comparator;
import java.util.List;

public class Main {

    public static void main(String[] args) {
//        Table users = MainTest.prepareUsuario(false);
//        Table cidades = MainTest.prepareCidade(false);
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

        Operator op = new TableScan(users, List.of("nome","idade","email","idCidade"));
        Operator op2 = new FilterOperator(op, new Filter<Tuple>() {
            @Override
            public boolean match(Tuple entries) {
                return entries.getContent("users").getInt("idade")>18;
            }
        });

        Operator cidadeScan = new TableScan(cidades);
        Operator join1 = new NestedLoopJoin(cidadeScan,op2, new ComparableFilter<Tuple>() {
            @Override
            public boolean match(Tuple t1, Tuple t2) {
                return t2.getContent("users").getInt("idCidade") ==
                        t1.getContent("cidades").getInt("id");
            }
        });

        Operator sorted = new SortOperator(join1, new Comparator<Tuple>() {
            @Override
            public int compare(Tuple o1, Tuple o2) {
                return o1.getContent("users").getString("name").compareTo(
                        o2.getContent("users").getString("name")
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

            row = t.getContent("cidades");
            str += "cidades.nome="+row.getString("nome")+" | ";
            str += "cidades.id="+row.getInt("id")+" | ";

            System.out.println(str);
        }
        executor.close();
        users.close();
        cidades.close();
    }
}
