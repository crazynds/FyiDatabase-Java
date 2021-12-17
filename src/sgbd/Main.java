package sgbd;

import sgbd.prototype.Column;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.table.SimpleTable;
import sgbd.table.Table;
import sgbd.util.Conversor;

import java.math.BigInteger;

public class Main {

    public static void main(String[] args) {
        //Prepara o protótipo da tabela
        Prototype p = new Prototype();
        p.addColumn("id",4,Column.PRIMARY_KEY);
        p.addColumn("nome",120,Column.DINAMIC_COLUMN_SIZE);
        p.addColumn("anoNascimento",4,Column.NONE);
        p.addColumn("email",120,Column.NONE);
        p.addColumn("idade",4,Column.CAM_NULL_COLUMN);
        p.addColumn("cidade",4,Column.PRIMARY_KEY);

        //Cria a tabela
        Table t = new SimpleTable(p);

        for (Column c:
             p) {
            System.out.println(c.getName()+" - "+c.getSize()+" - {PK: "+c.isPrimaryKey()+", SZ: "+c.getSize()+", DINAMIC: "+c.isDinamicSize()+" }");
        }

        t.open();
        //Criar itens custom

        RowData row = new RowData();
        row.addData("id", Conversor.intToByteArray(1));
        row.addData("cidade",Conversor.intToByteArray(3));
        row.addData("idade",Conversor.intToByteArray(21));
        row.addData("nome",Conversor.stringToByteArray("Luiz Henrique"));
        row.addData("email",Conversor.stringToByteArray("Luiz Henrique"));

        BigInteger pk =t.insert(row);
        System.out.println(pk.longValue());


        t.close();

    }
}
