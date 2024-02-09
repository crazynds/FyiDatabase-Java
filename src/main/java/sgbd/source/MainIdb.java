package sgbd.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.prototype.column.FloatColumn;
import sgbd.prototype.column.IntegerColumn;
import sgbd.prototype.column.StringColumn;
import sgbd.query.TestOperators;
import sgbd.query.sourceop.SourceScan;
import sgbd.source.components.Header;
import sgbd.util.global.Faker;

import java.util.Random;
import lib.BigKey;
import sgbd.prototype.TranslatorApi;
import sgbd.prototype.column.Column;
import sgbd.source.table.GenericTable1;
import sgbd.util.global.Util;

public class MainIdb {

    public static void printRecord(GenericTable1 tab, int id) {

        System.out.println("printing record ");
        
        RowData row = new RowData();
        row.setInt("id", id);
            
        BigKey bigKey = tab.translatorApi.getPrimaryKey(row);
        row = tab.findByRef(bigKey);
        
        List<String> columns = new ArrayList<String>();

        for (Column c : tab.translatorApi) {
            if (c.ignore()) {
                continue;
            }
            columns.add(c.getName());
        }

        String str = "";
        for (String col : columns) {
            switch (Util.typeOfColumn(row.getMetadata(col))) {
                case "boolean":
                    str += col + "=" + row.getBoolean(col) + "(" + Util.typeOfColumn(row.getMetadata(col)) + ")";
                    break;
                case "long":
                    str += col + "=" + row.getLong(col) + "(" + Util.typeOfColumn(row.getMetadata(col)) + ")";
                    break;
                case "int":
                    str += col + "=" + row.getInt(col) + "(" + Util.typeOfColumn(row.getMetadata(col)) + ")";
                    break;
                case "float":
                    str += col + "=" + row.getFloat(col) + "(" + Util.typeOfColumn(row.getMetadata(col)) + ")";
                    break;
                case "double":
                    str += col + "=" + row.getDouble(col) + "(" + Util.typeOfColumn(row.getMetadata(col)) + ")";
                    break;
                case "null":
                    str += col + "= Null (" + Util.typeOfColumn(row.getMetadata(col)) + ")";
                    break;
                case "string":
                default:
                    str += col + "=" + row.getString(col) + "(" + Util.typeOfColumn(row.getMetadata(col)) + ")";
                    break;
            }
            str += " | ";
        }
        System.out.println(str);
    }

    public static void main(String[] args) throws Exception {
        Prototype pt = new Prototype();

        pt.addColumn(new IntegerColumn("id", true));
        pt.addColumn(new IntegerColumn("semestre"));

        pt.addColumn(new StringColumn("nome"));
        pt.addColumn(new IntegerColumn("idade"));
        pt.addColumn(new FloatColumn("salario"));
        pt.addColumn(new IntegerColumn("num_cart"));
        Header header = new Header(pt, "pessoa_teste");
        GenericTable1 tab = new GenericTable1(header, "d:\\", "newTable", 4096, false);

        tab.open();
        Faker.replaceRandom(new Random(1000));
        for (int x = 1; x <= 30; x++) {
            RowData row = new RowData();
            row.setInt("id", x * 100);
            row.setInt("semestre", Faker.integer(0, 3));
            row.setString("nome", Faker.firstName());
            row.setInt("idade", Faker.integer(20, 50));
            row.setFloat("salario", Faker.floatPoint(3000, 10000));
            row.setInt("num_cart", Faker.integer(1000000, 100000000));

            Integer i = row.getInt("id");
            System.out.println("adicionando id = " + i);
            tab.insert(row);
        }

        printRecord(tab, 20);
        
        //TestOperators.testOperator(new SourceScan(tab));

        tab.close();
    }
}
