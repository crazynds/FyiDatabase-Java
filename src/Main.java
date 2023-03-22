import sgbd.table.Table;

public class Main {

    public static Table openUser(){
        Table tableUsers = Table.loadFromHeader("users.head");
        return tableUsers;
    }
    public static Table openCidade(){
        Table tableCidades = Table.loadFromHeader("cidades.head");
        return tableCidades;
    }


    public static void main(String[] args){
        Table t = Table.loadFromHeader("BIOSTATS.head");
    }
}
