package sgbd.query;

import engine.info.Parameters;
import sgbd.info.Query;
import sgbd.prototype.ComplexRowData;
import sgbd.query.agregation.AvgAgregation;
import sgbd.query.agregation.CountAgregation;
import sgbd.query.agregation.MaxAgregation;
import sgbd.query.agregation.MinAgregation;
import sgbd.query.binaryop.joins.NestedLoopJoin;
import sgbd.query.sourceop.TableScan;
import sgbd.query.unaryop.GroupOperator;
import sgbd.table.Table;
import sgbd.util.statitcs.Util;

import java.util.*;

public class Main {

    public static void main(String[] args) {

        Table cidades = Table.loadFromHeader("cidades.head");
        Table users = Table.loadFromHeader("users.head");

        cidades.open();
        users.open();

        Operator cidadeTable = new TableScan(cidades);
        Operator userTable = new TableScan(users);

        Operator join = new NestedLoopJoin(cidadeTable,userTable,(t1, t2) -> {
            Integer a = t1.getContent("cidades").getInt("id");
            Integer b = t2.getContent("users").getInt("idCidade");
            return a==b;
        });

        Operator group = new GroupOperator(join,"cidades","id",List.of(
            new MinAgregation("users","salario"),
            new MaxAgregation("users","salario"),
            new AvgAgregation("users","id"),
            new CountAgregation("users","id")
        ));

        Operator executor=group;

        for(Map.Entry<String, List<String>> content: executor.getContentInfo().entrySet()){
            for(String col:content.getValue()){
                System.out.print(content.getKey()+"."+col+" |");
            }
        }
        System.out.println();

        executor.open();
        while (executor.hasNext()) {
            Tuple t = executor.next();
            String str = "";
            for(Map.Entry<String, List<String>> content: executor.getContentInfo().entrySet()){
                for(String col:content.getValue()){
                    ComplexRowData row = t.getContent(content.getKey());
                    switch (Util.typeOfColumn(row.getMeta(col))) {
                        case "int":
                            str += content.getKey() + "." + col + "=" + row.getInt(col);
                            break;
                        case "float":
                            str += content.getKey() + "." + col + "=" + row.getFloat(col);
                            break;
                        case "double":
                            str += content.getKey() + "." + col + "=" + row.getDouble(col);
                            break;
                        case "string":
                        default:
                            str += content.getKey() + "." + col + "=" + row.getString(col);
                            break;
                    }
                    str += " | ";
                }
            }
            System.out.println(str);
        }
        //Fecha operador
        executor.close();


        //Fecha as tables, não serão mais acessadas


        System.out.println("");
        System.out.println("");

        System.out.println("Query performance: ");
        System.out.println("Buscas por chave primaria: "+ Query.PK_SEARCH);
        System.out.println("Tuplas sorteadas: "+Query.SORT_TUPLES);
        System.out.println("Comparações de FILTER: "+Query.COMPARE_FILTER);
        System.out.println("Comparações de JOIN: "+Query.COMPARE_JOIN);
        System.out.println("Comparações de TUPLAS DISTINTAS: "+Query.COMPARE_DISTINCT_TUPLE);

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
