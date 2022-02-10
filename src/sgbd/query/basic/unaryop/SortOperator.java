package sgbd.query.basic.unaryop;

import sgbd.info.Query;
import sgbd.prototype.RowData;
import sgbd.query.basic.Operator;
import sgbd.query.basic.Tuple;

import java.util.*;

public class SortOperator extends UnaryOperation{

    private Iterator<Tuple> iterator;
    private String source, column;
    private Comparator<Tuple> comparator;

    public SortOperator(Operator op,Comparator<Tuple> comparator) {
        super(op);
        this.source=source;
        this.column=column;
        this.comparator = comparator;
    }

    @Override
    public void open() {
        List<Tuple> itens = new ArrayList<>();
        operator.open();
        while (operator.hasNext()){
            itens.add(operator.next());
        }
        operator.close();
        itens.sort(comparator);
        Query.SORT_TUPLES += itens.size();
        iterator = itens.iterator();
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