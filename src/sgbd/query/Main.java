package sgbd.query;

import engine.info.Parameters;
import sgbd.info.Query;
import sgbd.prototype.Column;
import sgbd.prototype.ComplexRowData;
import sgbd.query.binaryop.UnionOperator;
import sgbd.query.binaryop.joins.BlockNestedLoopJoin;
import sgbd.query.sourceop.TableScan;
import sgbd.query.unaryop.FilterColumnsOperator;
import sgbd.query.unaryop.FilterOperator;
import sgbd.table.Table;
import sgbd.util.Util;

import java.util.*;

public class Main {

    public static void main(String[] args) {

        Table proj = Table.loadFromHeader("proj.head");
        Table aloc = Table.loadFromHeader("aloc.head");
        Table func = Table.loadFromHeader("func.head");
        Table depto = Table.loadFromHeader("depto.head");

        proj.open();
        aloc.open();
        func.open();
        depto.open();

        Operator projTable = new TableScan(proj);
        Operator alocTable = new TableScan(aloc);
        Operator funcTable = new TableScan(func,List.of("FUNC_idChefe"));
        Operator deptoTable = new TableScan(depto,List.of("DEPTO_idDiretor"));


        Operator union = new UnionOperator(funcTable,deptoTable,List.of("func.FUNC_idChefe"),List.of("depto.DEPTO_idDiretor"));

        Operator executor=union;


        for (Column c:
                func.getTranslator()) {
            System.out.println("Nome: "+c.getName()+" Tipo: "+Util.typeOfColumn(c)+" Tamanho: "+c.getSize());
        }
        for (Column c:
                depto.getTranslator()) {
            System.out.println("Nome: "+c.getName()+" Tipo: "+Util.typeOfColumn(c)+" Tamanho: "+c.getSize());
        }

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
            for (Map.Entry<String, ComplexRowData> row : t) {
                for (Map.Entry<String, byte[]> data : row.getValue()) {
                    switch (Util.typeOfColumn(row.getValue().getMeta(data.getKey()))) {
                        case "int":
                            str += row.getKey() + "." + data.getKey() + "=" + row.getValue().getInt(data.getKey());
                            break;
                        case "float":
                            str += row.getKey() + "." + data.getKey() + "=" + row.getValue().getFloat(data.getKey());
                            break;
                        case "double":
                            str += row.getKey() + "." + data.getKey() + "=" + row.getValue().getDouble(data.getKey());
                            break;
                        case "string":
                        default:
                            str += row.getKey() + "." + data.getKey() + "=" + row.getValue().getString(data.getKey());
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
