package sgbd.query.binaryop.joins;

import sgbd.info.Query;
import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.util.interfaces.ComparableFilter;

public class LeftNestedLoopJoin extends NestedLoopJoin{

    protected int qtdFinded = 0;
    public LeftNestedLoopJoin(Operator left, Operator right, ComparableFilter<Tuple> comparator) {
        super(left, right, comparator);
    }
    public LeftNestedLoopJoin(Operator left, Operator right) {
        super(left, right);
    }

    @Override
    public void open() {
        super.open();
        qtdFinded = 0;
    }

    @Override
    protected Tuple findNextTuple(){
        //Executa apenas quando o next tuple não existe
        if(nextTuple!=null)return nextTuple;
        //Loopa pelo operador esquerdo
        while(currentLeftTuple!=null || left.hasNext()){
            if(currentLeftTuple==null){
                currentLeftTuple = left.next();
                right.open();
                qtdFinded = 0;
            }
            //Loopa pelo operador direito
            while(right.hasNext()){
                Tuple rightTuple = right.next();
                //Faz a comparação do join
                Query.COMPARE_JOIN++;
                if(comparator==null || comparator.match(currentLeftTuple,rightTuple)){
                    qtdFinded++;
                    nextTuple = new Tuple(currentLeftTuple,rightTuple);
                    return nextTuple;
                }
            }
            right.close();
            if(qtdFinded==0){
                nextTuple = currentLeftTuple;
                currentLeftTuple = null;
                return nextTuple;
            }else currentLeftTuple=null;
        }
        return null;
    }
}
