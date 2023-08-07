package crazynds.fyidatabase.sgbd.query;

import org.junit.Test;
import sgbd.prototype.column.Column;
import sgbd.prototype.Prototype;
import sgbd.query.Operator;
import sgbd.query.binaryop.joins.LeftNestedLoopJoin;
import sgbd.query.binaryop.joins.NestedLoopJoin;
import sgbd.query.binaryop.joins.RightNestedLoopJoin;
import sgbd.query.sourceop.TableScan;
import sgbd.table.Table;
import sgbd.table.components.Header;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class OrderTest {

    private Table getUserTable(){
        Prototype p1 = new Prototype();
        p1.addColumn("id",4,Column.PRIMARY_KEY);
        p1.addColumn("nome",255,Column.DINAMIC_COLUMN_SIZE|Column.STRING);
        p1.addColumn("anoNascimento",4,Column.NONE);
        p1.addColumn("email",120,Column.STRING);
        p1.addColumn("idade",4,Column.CAN_NULL_COLUMN);
        p1.addColumn("salario",4,Column.FLOATING_POINT);
        p1.addColumn("idCidade",4,Column.NONE);
        Header h = new Header(p1,"users");
        h.set(Header.TABLE_TYPE,"MemoryTable");
        Table tableUsers = Table.openTable(h,true);
        return tableUsers;
    }

    private Table getCidadeTable(){
        Prototype p1 = new Prototype();
        p1.addColumn("id",4, Column.PRIMARY_KEY);
        p1.addColumn("nome",255,Column.DINAMIC_COLUMN_SIZE);
        Header h = new Header(p1,"cidades");
        h.set(Header.TABLE_TYPE,"MemoryTable");
        Table tableCidades = Table.openTable(h,true);
        return tableCidades;
    }


    @Test
    public void testJoinOperator(){
        Table usersTable = getUserTable();
        Table cidadeTable = getCidadeTable();

        Operator scan1 = new TableScan(usersTable);
        Operator scan2 = new TableScan(cidadeTable);

        Operator join1 = new LeftNestedLoopJoin(scan1,scan2, null);
        Operator join2 = new RightNestedLoopJoin(scan1,scan2, null);
        Operator join3 = new NestedLoopJoin(scan1,scan2);

        ArrayList<String> arr = new ArrayList<>();
        Map<String, List<String>> test= join1.getContentInfo();
        for (Map.Entry<String,List<String>> e:
             test.entrySet()) {
            for (String s:
                 e.getValue()) {
                arr.add(e.getKey()+'.'+s);
            }
        }
        test= join2.getContentInfo();
        int x=0;
        for (Map.Entry<String,List<String>> e:
                test.entrySet()) {
            for (String s:
                    e.getValue()) {
                String val = e.getKey()+'.'+s;
                assertTrue(val.compareTo(arr.get(x))==0);
                x++;
            }
        }
        test= join3.getContentInfo();
        x=0;
        for (Map.Entry<String,List<String>> e:
                test.entrySet()) {
            for (String s:
                    e.getValue()) {
                String val = e.getKey()+'.'+s;
                assertTrue(val.compareTo(arr.get(x))==0);
                x++;
            }
        }
    }

}
