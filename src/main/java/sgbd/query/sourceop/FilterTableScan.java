package sgbd.query.sourceop;

import sgbd.info.Query;
import sgbd.query.Tuple;
import sgbd.table.Table;
import sgbd.util.Filter;

import java.util.List;

public class FilterTableScan extends TableScan{

    private Filter<Tuple> tupleFilter;
    private Tuple nextTuple = null;

    public FilterTableScan(Table t, Filter<Tuple> tupleFilter) {
        super(t);
        this.tupleFilter = tupleFilter;
    }

    public FilterTableScan(Table t, List<String> columns, Filter<Tuple> tupleFilter) {
        super(t, columns);
        this.tupleFilter = tupleFilter;
    }

    @Override
    public void open() {
        super.open();
        nextTuple=null;
    }

    @Override
    public Tuple next() {
        try {
            return findNextTuple();
        }finally {
            nextTuple = null;
        }
    }

    @Override
    public boolean hasNext() {
        return findNextTuple()!=null;
    }

    private Tuple findNextTuple(){
        if(nextTuple!=null)return nextTuple;
        while (super.hasNext()){
            Tuple temp = super.next();
            Query.COMPARE_FILTER++;
            if(tupleFilter.match(temp)) {
                nextTuple = temp;
                return nextTuple;
            }
        }
        return null;
    }

    @Override
    public void close() {
        super.close();
        nextTuple=null;
    }
}
