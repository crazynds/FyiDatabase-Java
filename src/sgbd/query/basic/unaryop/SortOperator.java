package sgbd.query.basic.unaryop;

import sgbd.info.Query;
import sgbd.prototype.RowData;
import sgbd.query.basic.Operator;
import sgbd.query.basic.Tuple;

import java.util.*;

public class SortOperator extends UnaryOperation{

    private Iterator<Tuple> iterator;
    private String source, column;

    public SortOperator(Operator op,String source,String column) {
        super(op);
        this.source=source;
        this.column=column;
    }

    @Override
    public void open() {
        List<Tuple> itens = new ArrayList<>();
        operator.open();
        while (operator.hasNext()){
            itens.add(operator.next());
        }
        operator.close();
        if(source=="*"){
            itens.sort((Tuple a, Tuple b)->{
                byte[] arrA=null;
                for (Map.Entry<String, RowData> entry:
                        a) {
                    arrA = entry.getValue().getData(column);
                    if(arrA!=null)break;
                }
                byte[] arrB=null;
                for (Map.Entry<String, RowData> entry:
                        b) {
                    arrB = entry.getValue().getData(column);
                    if(arrB!=null)break;
                }
                if(arrA==null && arrB==null)return 0;
                if(arrA==null)return -1;
                if(arrB==null)return 1;
                Query.SORT_COMPARATORS++;
                return compare(arrA,arrB);
            });
        }else{
            itens.sort((Tuple a, Tuple b)->{
                byte[] arrA = a.getContent(source).getData(column);
                byte[] arrB = b.getContent(source).getData(column);
                Query.SORT_COMPARATORS++;
                return compare(arrA,arrB);
            });
        }
        Query.SORT_TUPLES+=itens.size();
        iterator = itens.iterator();
    }
    //lexicographic
    private int compare(byte[] left, byte[] right) {
        if(left==right)return 0;
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }

    @Override
    public Tuple next() {
        return iterator.next();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public void close() {
        iterator=null;
    }
}
