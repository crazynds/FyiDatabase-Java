package sgbd.query;

import engine.info.Parameters;
import sgbd.info.Query;
import sgbd.prototype.Column;
import sgbd.prototype.ComplexRowData;
import sgbd.query.agregation.AvgAgregation;
import sgbd.query.agregation.CountAgregation;
import sgbd.query.agregation.MaxAgregation;
import sgbd.query.agregation.MinAgregation;
import sgbd.query.binaryop.joins.BlockNestedLoopJoin;
import sgbd.query.binaryop.joins.NestedLoopJoin;
import sgbd.query.sourceop.TableScan;
import sgbd.query.unaryop.*;
import sgbd.table.Table;
import sgbd.util.interfaces.Conversor;
import sgbd.util.statitcs.Util;

import java.util.*;

public class Main {

    public static void main(String[] args) {

        Table tab1 = Table.loadFromHeader("biostats.head");
        Table tab2 = Table.loadFromHeader("mlbplayers.head");
        Table biostats = Table.loadFromHeader("biostats.head");
        Table mlbplayers = Table.loadFromHeader("mlbplayers.head");

        biostats.open();
        mlbplayers.open();

        Operator scan1 = new TableScan(biostats);

        Operator sanitize = new SanitizationOperator(scan1,t -> {
            ComplexRowData row = t.getContent("biostats");
            t.setContent("biostats_2",row);
            t.setContent("biostats",null);

            return t; // aqui tu pode retornar uma tupla nova tambem
            // return new Tuple();
        });

        Operator exec = sanitize;


        TestOperators.testOperator(exec,15); // Executa select por 15 itens
        TestOperators.testOperator(exec); // Executa select por 15 itens

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
