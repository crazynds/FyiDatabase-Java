package sgbd.query.binaryop;

import engine.exceptions.DataBaseException;
import sgbd.prototype.Column;
import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.query.unaryop.DistinctOperator;
import sgbd.util.ComparableFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UnionOperator extends BinaryOperator{

    private ArrayList<Tuple> tuples = new ArrayList<>();
    private ComparableFilter<Tuple> comparator;
    private List<String> leftColumns = null, rightColumns = null;
    private boolean isRight = false;

    private Tuple nextTuple = null;

    public UnionOperator(Operator left, Operator right) {
        super(left, right);
        comparator = (t1, t2) -> {
            return t1.compareTo(t2) == 0 && t2.compareTo(t1) == 0;
        };
    }
    public UnionOperator(Operator left, Operator right, List<String> leftColumns, List<String> rightColumns) {
        super(new DistinctOperator(left), new DistinctOperator(right));
        this.leftColumns = leftColumns;
        this.rightColumns = rightColumns;
        if(leftColumns.size()!=rightColumns.size())throw new DataBaseException("UnionOperator->Constructor","As listas de colunas ter a mesma quantidade de argumentos");
        for(int x=0;x< leftColumns.size();x++){
            String a[] = leftColumns.get(x).split("\\.");
            String b[] = rightColumns.get(x).split("\\.");
            if(a.length!=2 || b.length!=2)throw new DataBaseException("UnionOperator->Constructor","As listas de colunas devem ter um 'source' e 'column' separados por um ponto. Ex: 'users.name'");
        }
        this.comparator = (t1, t2) -> {
            for(int x=0;x< leftColumns.size();x++){
                String a[] = leftColumns.get(x).split("\\.");
                String b[] = rightColumns.get(x).split("\\.");
                if(
                        Arrays.compare(
                            t1.getContent(a[0]).getData(a[1]),
                            t2.getContent(b[0]).getData(b[1])
                        )!=0
                )return false;
            }
            return false;
        };
    }

    @Override
    public void open() {
        tuples.clear();
        left.open();
        right.open();
        isRight = false;
    }

    private boolean checkValid(Tuple newTuple){
        for (Tuple t:
             tuples) {
            if(comparator.match(t,newTuple))return false;
        }
        return true;
    }

    private Tuple getNextTuple(){
        if(nextTuple != null)return nextTuple;
        while(left.hasNext()){
            nextTuple = left.next();
            tuples.add(nextTuple);
            return nextTuple;
        }
        while(right.hasNext()){
            nextTuple = right.next();
            isRight = true;
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
        if(leftColumns!=null){
            Tuple n = new Tuple();
            for (int x=0;x< leftColumns.size();x++) {
                String[] pt = leftColumns.get(x).split("\\.");
                byte[] data;
                Column meta;
                if(isRight){
                    String[] pt2 = rightColumns.get(x).split("\\.");
                    data = t.getContent(pt2[0]).getData(pt2[1]);
                    meta = t.getContent(pt2[0]).getMeta(pt2[1]);
                }else{
                    data = t.getContent(pt[0]).getData(pt[1]);
                    meta = t.getContent(pt[0]).getMeta(pt[1]);
                }
                if(meta!=null || data!=null)
                    n.getContent(pt[0]).setData(pt[1],data,meta);
            }
            t = n;
        }
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
