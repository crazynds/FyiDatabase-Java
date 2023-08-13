package sgbd.query;

import engine.info.Parameters;
import lib.booleanexpression.entities.elements.Value;
import lib.booleanexpression.entities.elements.Variable;
import lib.booleanexpression.entities.expressions.AtomicExpression;
import lib.booleanexpression.entities.expressions.BooleanExpression;
import lib.booleanexpression.entities.expressions.LogicalExpression;
import lib.booleanexpression.enums.LogicalOperator;
import lib.booleanexpression.enums.RelationalOperator;
import sgbd.info.Query;
import sgbd.prototype.BData;
import sgbd.prototype.metadata.IntegerMetadata;
import sgbd.prototype.query.fields.IntegerField;
import sgbd.query.binaryop.joins.LeftNestedLoopJoin;
import sgbd.query.sourceop.TableScan;
import sgbd.query.unaryop.*;
import sgbd.table.Table;
import sgbd.util.global.UtilConversor;

import java.util.*;

public class Main {

    public static void main(String[] args) {

        Table movie = Table.loadFromHeader("filme.head");
        movie.open();

        Operator movie1 = new TableScan(movie);
        Operator movie2 = new TableScan(movie);


        BooleanExpression b1 = new AtomicExpression(
                new Variable("filme.idFilme"),
                new Value(new IntegerField(10)),
                RelationalOperator.GREATER_THAN_OR_EQUAL);
        BooleanExpression b2 = new AtomicExpression(
                new Variable("filme.idFilme"),
                new Value(new IntegerField(4)),
                RelationalOperator.LESS_THAN);

        BooleanExpression b = new LogicalExpression(LogicalOperator.OR,b1,b2);

        Operator filterOperator = new FilterOperator(movie1,b);


        TestOperators.testOperator(filterOperator);

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
