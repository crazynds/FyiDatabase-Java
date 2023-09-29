package sgbd.source;

import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.prototype.column.FloatColumn;
import sgbd.prototype.column.IntegerColumn;
import sgbd.prototype.column.StringColumn;
import sgbd.query.TestOperators;
import sgbd.query.sourceop.SourceScan;
import sgbd.source.components.Header;
import sgbd.source.table.SimpleTable;
import sgbd.source.table.Table;
import sgbd.util.global.Faker;

import java.util.Random;

public class Main {

    public static void main(String[] args) {
        Prototype pt = new Prototype();


        pt.addColumn(new IntegerColumn("id",true));
        pt.addColumn(new IntegerColumn("semestre",true));


        pt.addColumn(new StringColumn("nome"));
        pt.addColumn(new IntegerColumn("idade"));
        pt.addColumn(new FloatColumn("salario"));
        pt.addColumn(new IntegerColumn("num_cart"));
        Header header = new Header(pt,"pessoa_teste");
        Table tab = new SimpleTable(header);

        tab.open();
        Faker.replaceRandom(new Random(1000));
        for(int x=1;x<=10;x++){
            RowData row = new RowData();
            row.setInt("id",Faker.integer(1000,2000));
            row.setInt("semestre",Faker.integer(0,3));
            row.setString("nome",Faker.firstName());
            row.setInt("idade",Faker.integer(20,50));
            row.setFloat("salario", Faker.floatPoint(3000,10000));
            row.setInt("num_cart",Faker.integer(1000000,100000000));
            tab.insert(row);
        }

        TestOperators.testOperator(new SourceScan(tab.getPrimaryIndex()));


        tab.close();
    }
}
