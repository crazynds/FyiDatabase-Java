import engine.file.FileManager;
import engine.file.blocks.Block;
import engine.file.buffers.FIFOBlockBuffer;
import engine.file.buffers.OptimizedFIFOBlockBuffer;
import sgbd.prototype.Column;
import sgbd.prototype.ComplexRowData;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;
import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.query.binaryop.NestedLoopJoin;
import sgbd.query.sourceop.TableScan;
import sgbd.query.unaryop.FilterOperator;
import sgbd.table.SimpleTable;
import sgbd.table.Table;
import sgbd.table.components.Header;
import sgbd.table.components.RowIterator;
import sgbd.util.Util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

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
        Table users = openUser();
        Table cidades = openCidade();
        users.open();
        cidades.open();

        // TableScan faz a leitura completa das tabelas
        Operator selectAllUsers = new TableScan(users);
        Operator selectAllCities = new TableScan(cidades);
        // FilterOperator remove os items que não passarem na condição
        Operator selectSomeUsers = new FilterOperator(selectAllUsers,(Tuple t)->{
            return t.getContent("users").getInt("idade") >=18;
        });
        Operator selectSomeCities = new FilterOperator(selectAllCities,(Tuple t)->{
            return t.getContent("cidades").getString("nome").compareToIgnoreCase("Santa Maria")==0;
        });

        // NestedLoopJoin faz a jução da tupla A com a tupla B se a condição for verdadeira
        Operator joinUsersCities = new NestedLoopJoin(selectSomeUsers,selectAllCities,(t1, t2) -> {
            return t1.getContent("users").getInt("idCidade") == t2.getContent("cidades").getInt("id");
        });

        Operator query = joinUsersCities;

        // Itera sobre cada linha dos operadores, note que operadores trabalham com Tupla's ao invez de RowData
        for (query.open(); query.hasNext(); ) {
            // Uma Tuple é um conjunto de um ou mais ComplexRowData, dependendo da quanitadade de joins que acontecerem
            Tuple tuple = query.next();
            String str = "";
            for (Map.Entry<String, ComplexRowData> row: tuple){
                for(Map.Entry<String,byte[]> data:row.getValue()) {
                    switch(Util.typeOfColumnByName(data.getKey())){
                        case "int":
                            str+=row.getKey()+"."+data.getKey()+"="+row.getValue().getInt(data.getKey());
                            break;
                        case "float":
                            str+=row.getKey()+"."+data.getKey()+"="+row.getValue().getFloat(data.getKey());
                            break;
                        case "string":
                        default:
                            str+=row.getKey()+"."+data.getKey()+"="+row.getValue().getString(data.getKey());
                            break;
                    }
                    str+=" | ";
                }
            }
            System.out.println(str);
        }

        users.close();
        cidades.close();
    }
}
