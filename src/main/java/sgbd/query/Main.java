package sgbd.query;

import engine.info.Parameters;
import sgbd.info.Query;
import sgbd.prototype.ComplexRowData;
import sgbd.query.agregation.AvgAgregation;
import sgbd.query.agregation.CountAgregation;
import sgbd.query.agregation.MaxAgregation;
import sgbd.query.agregation.MinAgregation;
import sgbd.query.binaryop.joins.BlockNestedLoopJoin;
import sgbd.query.binaryop.joins.NestedLoopJoin;
import sgbd.query.sourceop.TableScan;
import sgbd.query.unaryop.FilterColumnsOperator;
import sgbd.query.unaryop.FilterOperator;
import sgbd.query.unaryop.GroupOperator;
import sgbd.query.unaryop.SelectColumnsOperator;
import sgbd.table.Table;
import sgbd.util.statitcs.Util;

import java.util.*;

public class Main {

    public static void main(String[] args) {

        Table elenco = Table.loadFromHeader("elenco.head");
        Table filme = Table.loadFromHeader("filme.head");

        elenco.open();
        filme.open();

        Operator elencoTable = new TableScan(elenco);
        Operator filmeTable = new TableScan(filme);

        Operator cartesiano = new BlockNestedLoopJoin(filmeTable,elencoTable,(t1, t2) -> true);
        Operator filter = new FilterOperator(cartesiano,entrie -> {
            return entrie.getContent("elenco").getLong("idFilme")==entrie.getContent("filme").getLong("idFilme");
        });

        Operator select1 = new FilterColumnsOperator(filter,List.of("elenco.idFilme","elenco.idAtor"));
        Operator select2 = new FilterColumnsOperator(select1,List.of("filme.bilheteria","filme.ano","filme.custo","filme.idDiretor"));


        TestOperators.testOperator(select2,15); // Executa select por 15 itens
        TestOperators.testOperator(select2);         // Executa select sem limit de selecao



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
