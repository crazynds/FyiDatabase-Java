package sgbd.test;

import sgbd.prototype.column.Column;
import sgbd.prototype.Prototype;
import sgbd.prototype.column.FloatColumn;
import sgbd.prototype.column.IntegerColumn;
import sgbd.prototype.column.StringColumn;
import sgbd.source.Source;
import sgbd.source.components.Header;
import sgbd.source.table.CompleteTable;
import sgbd.source.table.GenericTable1;
import sgbd.source.table.Table;

import java.util.HashMap;

public class Main {

    public static void main(String[] args) throws Exception {
        HashMap<String,String> mapa = new HashMap<>();
        Prototype p1 = new Prototype();
        p1.addColumn(new IntegerColumn("id",true));
        mapa.put("id","int");
        p1.addColumn(new StringColumn("nome"));
        mapa.put("nome","string");
        p1.addColumn(new IntegerColumn("idade"));
        mapa.put("idade","int");
        p1.addColumn(new FloatColumn("salario"));
        mapa.put("salario","float");

        //Source users = new CompleteTable(new Header(p1,"users"));
        Source users = new GenericTable1(new Header(p1,"users"), "d:\\", "newTable", 4096, true);

        ConsistenceTest consistenceTest = new ConsistenceTest(users,mapa,156);
        int qtdData=1000000;
        int block = 1000;
        long startTime = System.nanoTime();
        //consistenceTest.generateRandomData(qtdData);
        consistenceTest.generateRandomDataBlock(qtdData,block);
        long generateTime = System.nanoTime();
        consistenceTest.printAllData();
        long startCheckinTime = System.nanoTime();
//        if(consistenceTest.checkConsistence(qtdData)){
//            System.out.println("Dados consistentes");
//        }
        long checkTime = System.nanoTime();

        users.close();

        System.out.println("Tempo de geração: "+(generateTime-startTime)/1000000000.0);
        System.out.println("Tempo de checagem: "+(checkTime-startCheckinTime)/1000000000.0);



        /**
         * Testes ANTIGOS
         */

        /**
         * Testes realizados da seguinte forma:
         * SSD
         * 1.000.000 de itens adicionados
         * 10.000 itens por bloco a ser inserido
         */

        /**
         * Simple data table results:
         * [1000000/1000000] 100.0% de records válidos verificados!
         * Dados consistentes
         * Tempo de geração: 237.3638839
         * Tempo de checagem: 67.5330893
         */

        /**
         * Double data table results:
         * [1000000/1000000] 100.0% de records válidos verificados!
         * Dados consistentes
         * Tempo de geração: 20.4829137
         * Tempo de checagem: 99.8067396
         */

        /**
         * Memory data table results:
         * [1000000/1000000] 100.0% de records válidos verificados!
         * Dados consistentes
         * Tempo de geração: 3.9517636
         * Tempo de checagem: 2.4388966
         */

        /**
         * Testes usando o novo método de escrita e leitura direta (Não mudo nada):
         */

        /**
         * Simple data table results:
         * [1000000/1000000] 100.0% de records válidos verificados!
         * Dados consistentes
         * Tempo de geração: 188.8646945
         * Tempo de checagem: 66.3966542
         */

        /**
         * Double data table results:
         * [1000000/1000000] 100.0% de records válidos verificados!
         * Dados consistentes
         * Tempo de geração: 20.3080704
         * Tempo de checagem: 97.7573857
         */

        /**
         * Memory data table results:
         * [1000000/1000000] 100.0% de records válidos verificados!
         * Dados consistentes
         * Tempo de geração: 4.0426391
         * Tempo de checagem: 2.49235
         */

        /**
         * BTree data table results:
         * [999997/1000000] 99.9997% de records válidos verificados!
         * Tempo de geração: 54.7251642
         * Tempo de checagem: 8.8203467
         */

        /**
         * BTree data table results:
         * [999997/1000000] 99.9997% de records válidos verificados!
         * Tempo de geração: 13.6002134
         * Tempo de checagem: 58.9323223
         */

    }
}
