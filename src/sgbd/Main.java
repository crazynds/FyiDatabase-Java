package sgbd;

import sgbd.prototype.Column;
import sgbd.prototype.Prototype;
import sgbd.table.SimpleTable;
import sgbd.table.Table;

public class Main {

    public static void main(String[] args) {
        //Prepara o protótipo da tabela
        Prototype p = new Prototype();
        p.addColumn("id",4,Column.PRIMARY_KEY);
        p.addColumn("name",120,Column.DINAMIC_COLUMN_SIZE);
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



        t.close();





    }
}
