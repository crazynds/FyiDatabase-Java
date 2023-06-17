package crazynds.fyidatabase.sgbd;

import org.junit.Test;
import sgbd.prototype.Column;
import sgbd.prototype.Prototype;
import sgbd.table.Table;
import sgbd.table.components.Header;
import sgbd.test.ConsistenceTest;

import java.util.HashMap;

import static org.junit.Assert.assertTrue;

public class TableTest {

    protected void testConsistence(String tableType){
        HashMap<String,String> mapa = new HashMap<>();
        Prototype p1 = new Prototype();
        p1.addColumn("id",4, Column.PRIMARY_KEY);
        mapa.put("id","int");
        p1.addColumn("nome",255,Column.DINAMIC_COLUMN_SIZE);
        mapa.put("nome","string");
        p1.addColumn("idade",4,Column.CAN_NULL_COLUMN);
        mapa.put("idade","int");
        p1.addColumn("salario",4,Column.FLOATING_POINT);
        mapa.put("salario","float");
        Header h = new Header(p1,"users");
        h.set(Header.TABLE_TYPE,tableType);

        Table users = Table.openTable(h);

        ConsistenceTest consistenceTest = new ConsistenceTest(users,mapa,156);
        int qtdData=1000;
        int block = 10;
        long startTime = System.nanoTime();
        consistenceTest.generateRandomDataBlock(qtdData,block);
        long generateTime = System.nanoTime();
        assertTrue(consistenceTest.checkConsistence(qtdData));
    }


    @Test
    public void testConsistenceSimpleTable(){
        this.testConsistence("SimpleTable");
    }

//    @Test
//    public void testConsistenceDoubleTable(){
//        this.testConsistence("DoubleTable");
//    }

    @Test
    public void testConsistenceMemoryBTreeTable(){
        this.testConsistence("MemoryTable");
    }

    @Test
    public void testConsistenceBTreeTable(){
        this.testConsistence("BTreeTable");
    }

//    @Test
//    public void testConsistenceBTreeDoubleTable(){
//        this.testConsistence("BTreeDoubleTable");
//    }
}
