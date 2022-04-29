package sgbd.query.sourceop;

import sgbd.info.Query;
import sgbd.prototype.ComplexRowData;
import sgbd.query.Tuple;
import sgbd.table.Table;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class PKFilterTableScan extends TableScan{

    private Tuple nextTuple;
    private BigInteger startPk;
    private BigInteger lastPk;

    public PKFilterTableScan(Table table, BigInteger startPk, BigInteger lastPk) {
        super(table);
        this.startPk = startPk;
        this.lastPk = lastPk;
    }
    public PKFilterTableScan(Table table,List<String> columns, BigInteger startPk, BigInteger lastPk) {
        super(table,columns);
        this.startPk = startPk;
        this.lastPk = lastPk;
    }

    @Override
    public void open() {
        super.open();
        if(startPk!=null)
            iterator.setPointerPk(startPk);
        nextTuple=null;
    }

    @Override
    public Tuple next() {
        try {
            return findNextTuple();
        }finally {
            nextTuple=null;
        }
    }

    @Override
    public boolean hasNext() {
        return findNextTuple()!=null;
    }

    private Tuple findNextTuple(){
        if(nextTuple!=null)return nextTuple;
        while (iterator.hasNext()){
            Map.Entry<BigInteger, ComplexRowData> temp = iterator.nextWithPk();
            if(lastPk!=null) {
                Query.COMPARE_FILTER++;
                if(temp.getKey().compareTo(lastPk)<=0) {
                    nextTuple =new Tuple();
                    nextTuple.setContent(sourceName(),temp.getValue());
                    return nextTuple;
                }else{
                    return null;
                }
            }else{
                nextTuple =new Tuple();
                nextTuple.setContent(sourceName(),temp.getValue());
                return nextTuple;
            }
        }
        return null;
    }

    @Override
    public void close() {
        super.close();
        nextTuple = null;
    }
}
