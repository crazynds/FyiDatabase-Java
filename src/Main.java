import engine.file.FileManager;
import engine.file.blocks.Block;
import engine.file.buffers.FIFOBlockBuffer;
import engine.file.buffers.OptimizedFIFOBlockBuffer;
import engine.util.Util;
import sgbd.prototype.Column;
import sgbd.prototype.ComplexRowData;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.query.sourceop.TableScan;
import sgbd.query.unaryop.FilterOperator;
import sgbd.table.SimpleTable;
import sgbd.table.Table;
import sgbd.table.components.RowIterator;

import java.io.IOException;
import java.util.Arrays;

public class Main {


    public static void main(String[] args){
        // Cria um protótipo da tabela com os campos id,nome,cidade,idade,salario
        Prototype p = new Prototype();
        p.addColumn("id", 4, Column.PRIMARY_KEY);
        p.addColumn("nome", 120, Column.DINAMIC_COLUMN_SIZE);
        p.addColumn("cidade", 120, Column.DINAMIC_COLUMN_SIZE|Column.CAN_NULL_COLUMN);
        p.addColumn("idade", 4, Column.NONE);
        p.addColumn("salario", 4, Column.NONE|Column.CAN_NULL_COLUMN);

        // Cria a tabela usando o algoritmo do simple table
        Table table = SimpleTable.openTable("table.db",p);

        // Abre a tabela e limpa todos os dados atuais
        table.open();

        // TableScan faz a leitura completa da tabela
        Operator selectAllUsers = new TableScan(table);
        // FilterOperator remove os items que não passarem na condição
        Operator selectSomeUsers = new FilterOperator(selectAllUsers,(Tuple t)->{
            return t.getContent("table.db").getInt("idade") >=18;
        });

        // Itera sobre cada linha dos operadores, note que operadores trabalham com Tupla's ao invez de RowData
        for (selectSomeUsers.open(); selectSomeUsers.hasNext(); ) {
            // Uma Tuple é um conjunto de um ou mais ComplexRowData, dependendo da quanitadade de joins que acontecerem
            Tuple tuple = selectSomeUsers.next();
            ComplexRowData row = tuple.getContent("table.db");
            System.out.println("Nome: " + row.getString("nome")
                    + ", Cidade: " + row.getString("cidade")
                    + ", Idade: " + row.getInt("idade")
                    + ", Salario: " + row.getFloat("salario"));
        }

        table.close();
    }
}
