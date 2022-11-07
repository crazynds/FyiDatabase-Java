package sgbd.test;

import sgbd.prototype.Column;
import sgbd.prototype.Prototype;
import sgbd.table.*;

import java.io.File;
import java.util.HashMap;

public class Main {

    public static void main(String[] args) {
        HashMap<String,String> mapa = new HashMap<>();
        Prototype p1 = new Prototype();
        p1.addColumn("id",4, Column.PRIMARY_KEY);
        mapa.put("id","int");
        p1.addColumn("nome",255,Column.DINAMIC_COLUMN_SIZE);
        mapa.put("nome","string");
        p1.addColumn("idade",4,Column.CAM_NULL_COLUMN);
        mapa.put("idade","int");
        p1.addColumn("salario",4,Column.NONE);
        mapa.put("salario","float");

        //Table users = new BTreeDoubleTable("users",p1);
        Table users = SimpleTable.openTable("users",p1);
        //Table users = BTreeTable.openTable("users",p1,true);

        ConsistenceTest consistenceTest = new ConsistenceTest(users,mapa,156);
        int qtdData=1000000;
        int block = 10000;
        long startTime = System.nanoTime();
        //consistenceTest.generateRandomData(qtdData);
        consistenceTest.generateRandomDataBlock(qtdData,block);
        long generateTime = System.nanoTime();
        //consistenceTest.printAllData();
        if(consistenceTest.checkConsistence(qtdData)){
            System.out.println("Dados consistentes");
        }
        long checkTime = System.nanoTime();

        System.out.println("Tempo de geração: "+(generateTime-startTime)/1000000000.0);
        System.out.println("Tempo de checagem: "+(checkTime-generateTime)/1000000000.0);

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
