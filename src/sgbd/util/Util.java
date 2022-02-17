package sgbd.util;

import java.text.Normalizer;

public class Util {


    public static String typeOfColumnByName(String columnName){
        columnName = Normalizer.normalize(columnName,Normalizer.Form.NFD);

        if(columnName.contains("id"))return "int";
        if(columnName.contains("idade"))return "int";
        if(columnName.contains("age"))return "int";
        if(columnName.contains("size"))return "int";
        if(columnName.contains("__aux"))return "int";
        if(columnName.contains("reference"))return "int";


        if(columnName.contains("salario"))return "float";
        if(columnName.contains("value"))return "float";
        if(columnName.contains("valor"))return "float";
        if(columnName.contains("money"))return "float";


        if(columnName.contains("name"))return "string";
        if(columnName.contains("nome"))return "string";
        if(columnName.contains("email"))return "string";
        if(columnName.contains("text"))return "string";;
        if(columnName.contains("description"))return "string";
        if(columnName.contains("descricao"))return "string";

        return "string";
    }
}
