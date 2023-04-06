package sgbd.query.binaryop;

import engine.exceptions.DataBaseException;
import sgbd.info.Query;
import sgbd.prototype.Column;
import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.query.unaryop.DistinctOperator;
import sgbd.util.ComparableFilter;

import java.util.*;

public class UnionOperator extends BinaryOperator{

    protected ArrayList<Tuple> tuples = new ArrayList<>();
    protected ComparableFilter<Tuple> comparator;
    protected List<String[]> leftColumns = null, rightColumns = null;
    protected boolean isRight = false;

    protected Tuple nextTuple = null;

    public UnionOperator(Operator left, Operator right) {
        super(new DistinctOperator(left), new DistinctOperator(right));
        comparator = (t1, t2) -> t1.compareTo(t2) == 0 && t2.compareTo(t1) == 0;
    }
    public UnionOperator(Operator left, Operator right, List<String> leftColumns, List<String> rightColumns) {
        super(new DistinctOperator(left), new DistinctOperator(right));
        if(leftColumns.size()!=rightColumns.size())throw new DataBaseException("UnionOperator->Constructor","As listas de colunas ter a mesma quantidade de argumentos");
        if(leftColumns.size()<=0)throw new DataBaseException("UnionOperator->Constructor","Lista de colunas não devem ser vazias!");
        ArrayList<String[]> l = new ArrayList<>(), r = new ArrayList<>();
        for(int x=0;x< leftColumns.size();x++){
            String a[] = leftColumns.get(x).split("\\.");
            String b[] = rightColumns.get(x).split("\\.");
            if(a.length!=2 || b.length!=2)throw new DataBaseException("UnionOperator->Constructor","As listas de colunas devem ter um 'source' e 'column' separados por um ponto. Ex: 'users.name'");
            l.add(a);
            r.add(b);
        }
        this.leftColumns = l;
        this.rightColumns = r;
        this.comparator = (t1, t2) -> {
            for(int x=0;x< this.leftColumns.size();x++){
                String a[] = this.leftColumns.get(x);
                String b[] = this.rightColumns.get(x);
                byte[] dataA = t1.getContent(a[0]).getData(a[1]);
                byte[] dataB = t2.getContent(b[0]).getData(b[1]);
                if(Arrays.compare(dataA,dataB)!=0)return false;
            }
            return true;
        };
    }

    @Override
    public void open() {
        tuples.clear();
        left.open();
        right.open();
        isRight = false;
    }

    protected boolean checkValid(Tuple newTuple){
        for (Tuple t:
             tuples) {
            Query.COMPARE_DISTINCT_TUPLE++;
            if(comparator.match(t,newTuple))return false;
        }
        return true;
    }

    protected Tuple getNextTuple(){
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
                String[] pt = leftColumns.get(x);
                byte[] data;
                Column meta;
                if(isRight){
                    String[] pt2 = rightColumns.get(x);
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


    @Override
    public Map<String, List<String>> getContentInfo() {
        if(leftColumns==null)
            return left.getContentInfo();
        Map<String, List<String>> lTable = left.getContentInfo();
        Map<String, List<String>> current = new HashMap<>();
        for (int x = 0; x < leftColumns.size(); x++) {
            if (current.get(leftColumns.get(x)[0]) == null)
                current.put(leftColumns.get(x)[0], new ArrayList<>());
        }
        for (int x = 0; x < leftColumns.size(); x++) {
            List<String> arr = current.get(leftColumns.get(x)[0]);
            arr.add(leftColumns.get(x)[1]);
        }
        return current;
    }
}
