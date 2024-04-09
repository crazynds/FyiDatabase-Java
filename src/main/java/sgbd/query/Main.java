package sgbd.query;

import engine.info.Parameters;
import lib.booleanexpression.entities.elements.Variable;
import lib.booleanexpression.entities.expressions.AtomicExpression;
import lib.booleanexpression.enums.RelationalOperator;
import sgbd.info.Query;
import sgbd.prototype.column.Column;
import sgbd.query.binaryop.joins.NestedLoopJoin;
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
        movies_cast.open();
        movies_movie.open();
        movies_person.open();
        movies_movie_crew.open();

        Operator cast = new TableScan(movies_cast);
        Operator movie = new TableScan(movies_movie);
        Operator person = new TableScan(movies_person);
        Operator crew = new TableScan(movies_movie_crew);

        Operator join2 = new NestedLoopJoin(cast,movie, new AtomicExpression(
                new Variable("movies_cast.movie_id"),
                new Variable("movies_movie.movie_id"), RelationalOperator.EQUAL));
        Operator join = new NestedLoopJoin(join2,person, new AtomicExpression(
                new Variable("movies_person.person_id"),
                new Variable("movies_cast.person_id"), RelationalOperator.EQUAL));


        Operator join3 = new NestedLoopJoin(join2,crew, new AtomicExpression(
                new Variable("movies_movie.movie_id"),
                new Variable("movie_crew.movie_id"), RelationalOperator.EQUAL));


        Operator sort = new SortOperator(join2);

        for(Column c:movies_person.getTranslator()){
            System.out.println(c.getName()+" "+c.isPrimaryKey());
        }

        TestOperators.testOperator(join);

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
