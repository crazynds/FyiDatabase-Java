package sgbd.query.binaryop.joins;

import lib.booleanexpression.entities.expressions.BooleanExpression;
import sgbd.prototype.RowData;
import sgbd.prototype.query.fields.NullField;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;
import sgbd.util.interfaces.ComparableFilter;

import java.util.List;
import java.util.Map;

public class LeftNestedLoopJoin extends NestedLoopJoin{

    protected boolean findAnyMatch = false;

    protected Tuple nullColumns = null;

    @Deprecated
    public LeftNestedLoopJoin(Operator left, Operator right, ComparableFilter<Tuple> comparator) {
        super(left, right, comparator);
    }

    public LeftNestedLoopJoin(Operator left, Operator right, BooleanExpression expression) {
        super(left, right, expression);
    }

    @Override
    public void open() {
        super.open();
        findAnyMatch = false;
        nullColumns = new Tuple();
        for(Map.Entry<String, List<String>> entry:getRightOperator().getContentInfo().entrySet()){
            RowData row = new RowData();
            for(String s:entry.getValue()){
                row.setField(s, NullField.generic);
            }
            nullColumns.setContent(entry.getKey(),row);
        }
    }

    @Override
    public Tuple getNextTuple(){
        //Loopa pelo operador esquerdo
        while(currentLeftTuple!=null || left.hasNext()){
            if(currentLeftTuple==null){
                currentLeftTuple = left.next();
                this.applyLookup(currentLeftTuple);
                right.open();
                findAnyMatch = false;
            }
            //Loopa pelo operador direito
            while(right.hasNext()){
                Tuple rightTuple = right.next();
                //Faz a comparação do join
                Tuple t =checkReturn(currentLeftTuple,rightTuple);
                if(t!=null){
                    findAnyMatch |= true;
                    return t;
                }
            }
            right.close();
            if(!findAnyMatch){
                Tuple t = currentLeftTuple;
                currentLeftTuple = null;
                return new Tuple(t,nullColumns);
            }else currentLeftTuple=null;
        }
        return null;
    }
}
