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
        Table t = Table.loadFromHeader("BIOSTATS.head");
    }
}
