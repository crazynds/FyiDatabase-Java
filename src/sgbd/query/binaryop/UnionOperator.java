package sgbd.query.binaryop;

import sgbd.query.Operator;
import sgbd.query.Tuple;

import java.util.ArrayList;

public class UnionOperator extends BinaryOperator{

    private ArrayList<Tuple> tuples = new ArrayList<>();

    private Tuple nextTuple = null;

    public UnionOperator(Operator left, Operator right) {
        super(left, right);
    }

    @Override
    public void open() {
        tuples.clear();
        left.open();
        right.open();
    }

    private boolean checkValid(Tuple newTuple){
        for (Tuple t:
             tuples) {
            if(t.compareTo(newTuple)==0 && newTuple.compareTo(t)==0)return false;
        }
        tuples.add(newTuple);
        return true;
    }

    private Tuple getNextTuple(){
        if(nextTuple != null)return nextTuple;
        while(left.hasNext()){
            nextTuple = left.next();
            if(checkValid(nextTuple)){
                return nextTuple;
            }else nextTuple = null;
        }
        while(right.hasNext()){
            nextTuple = right.next();
            if(checkValid(nextTuple)){
                return nextTuple;
            }else nextTuple = null;
        }
        return nextTuple;
    }

    @Override
    public Tuple next() {
        Tuple t = getNextTuple();
        nextTuple = null;
        return t;
    }

    @Override
    public boolean hasNext() {
        return getNextTuple() != null;
    }

    @Override
    public void close() {
        tuples.clear();
        left.close();
        right.close();
    }
}
