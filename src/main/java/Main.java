import sgbd.source.table.Table;
import sgbd.util.classes.CSVRecognizer;
import sgbd.util.classes.InvalidCsvException;

import java.io.FileNotFoundException;

public class Main {

    public static Table openUser(){
        Table tableUsers = Table.loadFromHeader("users.head");
        return tableUsers;
    }
    public static Table openCidade(){
        Table tableCidades = Table.loadFromHeader("cidades.head");
        return tableCidades;
    }


    public static void main(String[] args) throws InvalidCsvException, FileNotFoundException {
        CSVRecognizer recognizer = new CSVRecognizer("people.csv",',','"', 1);
        for (String dado:
                recognizer.getColumnNames()) {
            System.out.print(dado+"\t*");
        }
        for (String[] linhas:
             recognizer) {
            for (String dado:
                 linhas) {
                System.out.print(dado+"\t|");
            }
            System.out.println();
        }
    }
}
