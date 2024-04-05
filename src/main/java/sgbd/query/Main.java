package sgbd.query;

import engine.info.Parameters;
import lib.booleanexpression.entities.constructor.ExpressionConstructor;
import lib.booleanexpression.entities.elements.Element;
import lib.booleanexpression.entities.elements.Value;
import lib.booleanexpression.entities.elements.Variable;
import lib.booleanexpression.entities.expressions.AtomicExpression;
import lib.booleanexpression.entities.expressions.BooleanExpression;
import lib.booleanexpression.entities.expressions.LogicalExpression;
import lib.booleanexpression.enums.LogicalOperator;
import lib.booleanexpression.enums.RelationalOperator;
import sgbd.info.Query;
import sgbd.prototype.query.fields.IntegerField;
import sgbd.query.binaryop.joins.NestedLoopJoin;
import sgbd.query.sourceop.TableScan;
import sgbd.query.unaryop.*;
import sgbd.source.table.Table;
import sgbd.util.classes.ResourceName;

import java.util.List;

public class Main {

    public static void main(String[] args) {

        Table table = Table.loadFromHeader("users.head");
        Table table2 = Table.loadFromHeader("cidades.head");
        table.open();
        table2.open();

        Operator scan = new TableScan(table);
        Operator scan2 = new TableScan(table2);

        BooleanExpression b = new AtomicExpression(
                new Variable("users.idCidade"),
                new Variable("cidades.id"), RelationalOperator.EQUAL);
        Operator sort = new SortOperator(scan,new ResourceName("users","idCidade"));
        Operator join = new NestedLoopJoin(sort,scan2,b);
        Operator filter = new SelectColumnsOperator(join, List.of("cidades.id","users.idCidade","users.nome"));

        TestOperators.testOperator(scan);

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
        System.out.println("Full Table Scan: "+Query.FULL_TABLE_SCAN);

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
