package sgbd.query;

import engine.info.Parameters;
import lib.booleanexpression.entities.elements.Null;
import lib.booleanexpression.entities.elements.Variable;
import lib.booleanexpression.entities.expressions.AtomicExpression;
import lib.booleanexpression.enums.RelationalOperator;
import sgbd.info.Query;
import sgbd.prototype.column.Column;
import sgbd.query.binaryop.joins.LeftNestedLoopJoin;
import sgbd.query.binaryop.joins.NestedLoopJoin;
import sgbd.query.sourceop.SourceScan;
import sgbd.query.sourceop.TableScan;
import sgbd.query.unaryop.*;
import sgbd.source.table.Table;

import java.util.List;

public class Main {

    public static void main(String[] args) {

        Table movies_cast = Table.loadFromHeader("movies_cast.head");
        Table movies_movie = Table.loadFromHeader("movies_movie.head");
        Table movies_person = Table.loadFromHeader("movies_person.head");
        Table movies_movie_crew = Table.loadFromHeader("movie_crew.head");

        Table idx_ano = Table.loadFromHeader("idx_ano.head");
        Table evasao2x = Table.loadFromHeader("evasao2X.head");

        idx_ano.open();
        evasao2x.open();
        movies_movie.open();
        movies_cast.open();
        movies_person.open();

        Operator scan1 = new SourceScan(idx_ano);
        Operator scan2 = new SourceScan(evasao2x);
        Operator scan3 = new SourceScan(movies_cast);
        Operator scan4 = new SourceScan(movies_person);

        Operator join = new LeftNestedLoopJoin(scan4,scan3,new AtomicExpression(
                new Variable("movies_cast.person_id"),
                new Variable("movies_person.person_id"),
                RelationalOperator.EQUAL
        ));

        Operator filter = new FilterOperator(join,new AtomicExpression(
                new Variable("movies_cast.movie_id"),
                new Null(),
                RelationalOperator.IS
        ));

        Operator select = new SelectColumnsOperator(filter,List.of("movies_person.person_id","movies_cast.movie_id"));


        for(Column c:movies_person.getTranslator()){
            System.out.println(c.getName()+" "+c.isPrimaryKey());
        }

        TestOperators.testOperator(select);

        //Fecha as tables, não serão mais acessadas


        System.out.println("");
        System.out.println("");

        System.out.println("Query performance: ");
        System.out.println("Buscas por chave primaria: "+ Query.PK_SEARCH);
        System.out.println("Tuplas sorteadas: "+Query.SORT_TUPLES);
        System.out.println("Comparações de FILTER: "+Query.COMPARE_FILTER);
        System.out.println("Comparações de JOIN: "+Query.COMPARE_JOIN);
        System.out.println("Comparações de TUPLAS DISTINTAS: "+Query.COMPARE_DISTINCT_TUPLE);
        System.out.println("Index UPPERBOUND: "+Query.LOOK_UP_UPPERBOUND);
        System.out.println("Index LOWERBOUND: "+Query.LOOK_UP_LOWERBOUND);
        System.out.println("Full table scan : "+Query.FULL_TABLE_SCAN);

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
        System.out.println("Memory by blocks: "+Parameters.MEMORY_ALLOCATED_BY_BLOCKS);
        System.out.println("Memory by byte arrays: "+Parameters.MEMORY_ALLOCATED_BY_BYTE_ARRAY);
        System.out.println("Memory by commitable blocks: "+Parameters.MEMORY_ALLOCATED_BY_COMMITTABLE_BLOCKS);
        System.out.println("Memory by direct blocks: "+Parameters.MEMORY_ALLOCATED_BY_DIRECT_BLOCKS);
        System.out.println("Memory by indirect blocks: "+Parameters.MEMORY_ALLOCATED_BY_INDIRECT_BLOCKS);
        System.out.println("Memory by records: "+Parameters.MEMORY_ALLOCATED_BY_RECORDS);
        System.out.println("Cache hit: "+Parameters.CACHE_HIT);
        System.out.println("Cache miss: "+Parameters.CACHE_MISS);
    }
}
