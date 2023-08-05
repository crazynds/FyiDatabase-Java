package sgbd.query;

import sgbd.prototype.RowData;
import sgbd.prototype.query.Tuple;
import sgbd.util.global.Util;

import java.util.List;
import java.util.Map;

public class TestOperators {

    public static void testOperator(Operator executor){
        testOperator(executor,-1);
    }
    public static void testOperator(Operator executor,long limit){
        System.out.println("Header:");

        for(Map.Entry<String, List<String>> content: executor.getContentInfo().entrySet()){
            for(String col:content.getValue()){
                System.out.print(content.getKey()+"."+col+" |");
            }
        }
        System.out.println();
        System.out.println("Body:");

        long count = 0;
        executor.open();
        while (executor.hasNext() && count!=limit) {
            Tuple t = executor.next();
            String str = "";
            for(Map.Entry<String, List<String>> content: executor.getContentInfo().entrySet()){
                for(String col:content.getValue()){
                    RowData row = t.getContent(content.getKey());
                    switch (Util.typeOfColumn(row.getMetadata(col))) {
                        case "boolean":
                            str += content.getKey() + "." + col + "=" + row.getBoolean(col)+"("+Util.typeOfColumn(row.getMetadata(col))+")";
                            break;
                        case "long":
                            str += content.getKey() + "." + col + "=" + row.getLong(col)+"("+Util.typeOfColumn(row.getMetadata(col))+")";
                            break;
                        case "int":
                            str += content.getKey() + "." + col + "=" + row.getInt(col)+"("+Util.typeOfColumn(row.getMetadata(col))+")";
                            break;
                        case "float":
                            str += content.getKey() + "." + col + "=" + row.getFloat(col)+"("+Util.typeOfColumn(row.getMetadata(col))+")";
                            break;
                        case "double":
                            str += content.getKey() + "." + col + "=" + row.getDouble(col)+"("+Util.typeOfColumn(row.getMetadata(col))+")";
                            break;
                        case "null":
                            str += content.getKey() + "." + col + "= Null ("+Util.typeOfColumn(row.getMetadata(col))+")";
                            break;
                        case "string":
                        default:
                            str += content.getKey() + "." + col + "=" + row.getString(col)+"("+Util.typeOfColumn(row.getMetadata(col))+")";
                            break;
                    }
                    str += " | ";
                }
            }
            System.out.println(str);
            count++;
        }
        //Fecha operador
        executor.close();
        executor.freeResources();
        System.out.println("Count: "+count);
    }


}
