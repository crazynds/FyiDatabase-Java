package sgbd.util.global;

import sgbd.prototype.metadata.Metadata;

import java.text.Normalizer;

public class Util {


    public static String typeOfColumnByName(String columnName){
        columnName = Normalizer.normalize(columnName,Normalizer.Form.NFD);

        if(columnName.contains("id"))return "int";
        if(columnName.contains("idade"))return "int";
        if(columnName.contains("age"))return "int";
        if(columnName.contains("ano"))return "int";
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

        return "binary";
    }

    public static String typeOfColumn(Metadata meta){
        if(meta == null) return "null";
        if(meta.isBoolean())return "boolean";
        if(meta.isString())return "string";
        if(meta.isInt() && meta.getSize()==8)return "long";
        if(meta.isInt() && meta.getSize()==4)return "int";
        if(meta.isFloat() && meta.getSize()==8)return "double";
        if(meta.isFloat() && meta.getSize()==4)return "float";
        return "binary";
    }
}
