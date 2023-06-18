package sgbd.query;

import sgbd.prototype.ComplexRowData;
import sgbd.util.statitcs.Util;

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
                    ComplexRowData row = t.getContent(content.getKey());
                    switch (Util.typeOfColumn(row.getMeta(col))) {
                        case "boolean":
                            str += content.getKey() + "." + col + "=" + row.getBoolean(col)+"("+Util.typeOfColumn(row.getMeta(col))+")";
                            break;
                        case "long":
                            str += content.getKey() + "." + col + "=" + row.getLong(col)+"("+Util.typeOfColumn(row.getMeta(col))+")";
                            break;
                        case "int":
                            str += content.getKey() + "." + col + "=" + row.getInt(col)+"("+Util.typeOfColumn(row.getMeta(col))+")";
                            break;
                        case "float":
                            str += content.getKey() + "." + col + "=" + row.getFloat(col)+"("+Util.typeOfColumn(row.getMeta(col))+")";
                            break;
                        case "double":
                            str += content.getKey() + "." + col + "=" + row.getDouble(col)+"("+Util.typeOfColumn(row.getMeta(col))+")";
                            break;
                        case "string":
                        default:
                            str += content.getKey() + "." + col + "=" + row.getString(col)+"("+Util.typeOfColumn(row.getMeta(col))+")";
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
        executor.clearTempFile();
        System.out.println("Count: "+count);
    }


}
