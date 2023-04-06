package sgbd;

import sgbd.prototype.*;
import sgbd.table.SimpleTable;
import sgbd.table.Table;
import sgbd.table.components.Header;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

public class MainTest {

    public static Table prepareCidade(boolean print){
        String name = "cidades";

        Prototype p2 = new Prototype();
        p2.addColumn("id",4,Column.PRIMARY_KEY);
        p2.addColumn("nome",255,Column.DINAMIC_COLUMN_SIZE);

        Table tableCidades = Table.openTable(new Header(p2,"cidades"),true);
        tableCidades.open();

        RowData cidade = new RowData();
        cidade.setInt("id",1);
        cidade.setString("nome","Santa Maria");
        tableCidades.insert(cidade);
        cidade.setInt("id",2);
        cidade.setString("nome","Canoas");
        tableCidades.insert(cidade);
        cidade.setInt("id",3);
        cidade.setString("nome","Alegrete");
        tableCidades.insert(cidade);
        cidade.setInt("id",4);
        cidade.setString("nome","Lavras");
        tableCidades.insert(cidade);
        cidade.setInt("id",5);
        cidade.setString("nome","Minas Gerais");
        tableCidades.insert(cidade);
        cidade.setInt("id",6);
        cidade.setString("nome","São Paulo");
        tableCidades.insert(cidade);
        cidade.setInt("id",7);
        cidade.setString("nome","Uberlandia");
        tableCidades.insert(cidade);
        cidade.setInt("id",8);
        cidade.setString("nome","Porto Alegre");
        tableCidades.insert(cidade);

        if(print) {
            for (Column c : p2) {
                System.out.println("Nome: " + c.getName() + " Type: " + (c.isDinamicSize() ? "Dinamic" : "Static") + " Size: " + c.getSize());
            }
            Iterator<ComplexRowData> i = tableCidades.iterator();
            int x = 1;
            while (i.hasNext()) {
                RowData r = i.next();
                System.out.println("Id: " + r.getInt("id")
                        + ", Nome: " + r.getString("nome"));
                x++;
            }
        }
        tableCidades.close();
        tableCidades.saveHeader("cidades.head");

        return Table.openTable(new Header(p2,"cidades"));
    }
    public static Table prepareUsuario(boolean print){
        String name = "users";
        Prototype p1 = new Prototype();
        p1.addColumn("id",4,Column.PRIMARY_KEY);
        p1.addColumn("nome",255,Column.DINAMIC_COLUMN_SIZE|Column.STRING);
        p1.addColumn("anoNascimento",4,Column.NONE);
        p1.addColumn("email",120,Column.STRING);
        p1.addColumn("idade",4,Column.CAN_NULL_COLUMN);
        p1.addColumn("salario",4,Column.FLOATING_POINT);
        p1.addColumn("idCidade",4,Column.NONE);

        Table tableUsers = Table.openTable(new Header(p1,"users"),true);
        Random rand = new Random();

        tableUsers.open();

        for (int x=0;x<35;x++) {
            RowData row = new RowData();
            row.setInt("id",x+1);
            row.setString("nome","Luiz "+x);
            row.setString("email","email@email"+x+".com");
            row.setInt("anoNascimento",1980+x);
            row.setFloat("salario",325.12f * (rand.nextInt(10)+5));
            row.setInt("idade",2022-(1980+x));
            row.setInt("idCidade", rand.nextInt(8)+1);
            tableUsers.insert(row);
        }

        if(print) {
            for (Column c : p1) {
                System.out.println("Nome: " + c.getName() + " Type: " + (c.isDinamicSize() ? "Dinamic" : "Static") + " Size: " + c.getSize());
            }

            Iterator<ComplexRowData> i = tableUsers.iterator();
            int x = 1;
            while (i.hasNext()) {
                RowData r = i.next();
                System.out.println("Nome: " + r.getString("nome")
                        + ", Email: " + r.getString("email")
                        + ", idCidade: " + r.getInt("idCidade")
                        + ", Idade: " + r.getInt("idade")
                        + ", Salario: " + r.getFloat("salario")
                        + ", AnoNascimento: " + r.getInt("anoNascimento"));
                x++;
            }
        }
        tableUsers.close();
        tableUsers.saveHeader("users.head");

        return Table.openTable(new Header(p1,"users"));
    }


    public static void main(String[] args) throws IOException {
        //Prepara o protótipo da tabela
        prepareCidade(true);
        prepareUsuario(true);
    }
}
