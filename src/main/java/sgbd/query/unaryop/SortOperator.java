package sgbd.query.unaryop;

import sgbd.info.Query;
import sgbd.prototype.query.fields.Field;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;
import sgbd.util.classes.ResourceName;

import java.util.*;

public class SortOperator extends UnaryOperator {

    private Iterator<Tuple> iterator;
    private Comparator<Tuple> comparator;

    @Deprecated
    public SortOperator(Operator op,Comparator<Tuple> comparator) {
        super(op);
        this.comparator = comparator;
    }

    public SortOperator(Operator op, ResourceName resourceName) {
        this(op,resourceName,resourceName);
    }
    public SortOperator(Operator op, ResourceName left,ResourceName right) {
        super(op);
        this.comparator = new Comparator<Tuple>() {
            @Override
            public int compare(Tuple o1, Tuple o2) {
                Field a = o1.getContent(left.getSource()).getField(left.getColumn());
                Field b = o2.getContent(right.getSource()).getField(right.getColumn());
                if(a!=null)
                    return a.compareTo(b);
                if(b!=null)
                    return b.compareTo(a);
                return 0;
            }
        };
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
