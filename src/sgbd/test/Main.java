package sgbd.test;

import sgbd.prototype.Column;
import sgbd.prototype.Prototype;
import sgbd.table.MemoryTable;
import sgbd.table.SimpleTable;
import sgbd.table.Table;

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

        Table users = new MemoryTable("users",p1);

        ConsistenceTest consistenceTest = new ConsistenceTest(users,mapa);
        int qtdData=100000;
        int block = 1000;

        //consistenceTest.generateRandomData(qtdData);
        consistenceTest.generateRandomDataBlock(qtdData,block);
        //consistenceTest.printAllData();
        if(consistenceTest.checkConsistence(qtdData)){
            System.out.println("Dados consistentes");
        }

    }
}
