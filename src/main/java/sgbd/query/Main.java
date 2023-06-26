package sgbd.query;

import engine.info.Parameters;
import sgbd.info.Query;
import sgbd.query.binaryop.joins.LeftNestedLoopJoin;
import sgbd.query.sourceop.TableScan;
import sgbd.query.unaryop.*;
import sgbd.table.Table;

import java.util.*;

public class Main {

    public static void main(String[] args) {

        Table movie = Table.loadFromHeader("filme.head");
        movie.open();

        Operator movie1 = new TableScan(movie);
        Operator movie2 = new TableScan(movie);

        Operator rename = new RenameSourceOperator(movie2, "filme", "filme2");

        Operator leftJoin = new LeftNestedLoopJoin(movie1, rename, (t1, t2) -> {
            return t1.getContent("filme").getInt("idFilme").equals(t2.getContent("filme2").getInt("idFilmeAnterior"));
        });

        Operator projection = new SelectColumnsOperator(leftJoin, List.of("filme.titulo","filme2.titulo"));

        TestOperators.testOperator(projection);

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
