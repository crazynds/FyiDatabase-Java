package sgbd.query.binaryop.joins;

import engine.exceptions.DataBaseException;
import engine.util.Util;
import sgbd.prototype.RowData;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;
import sgbd.query.sourceop.PKTableScan;

import java.math.BigInteger;
import java.util.Map;


@Deprecated
public class FKNestedLoopJoin extends NestedLoopJoin {

    protected String source,foreignKey;
    private boolean showIfNullRightOperator;

    public FKNestedLoopJoin(Operator left, PKTableScan tableScan, String source, String foreignKey) {
        super(left, tableScan);
        this.source=source;
        this.showIfNullRightOperator=false;
        this.foreignKey=foreignKey;
    }
    public FKNestedLoopJoin(Operator left, PKTableScan tableScan, String source, String foreignKey, boolean showIfNullRightOperator) {
        super(left, tableScan);
        this.source=source;
        this.showIfNullRightOperator=showIfNullRightOperator;
        this.foreignKey=foreignKey;
    }

    @Override
    public void setRightOperator(Operator op) {
        if(!(op instanceof PKTableScan))throw new DataBaseException("FKNestedLoopJoin->setRightOperator","Aceita apenas operadores do tipo PKTableScan");
        super.setRightOperator(op);
    }

    @Override
    public Tuple getNextTuple(){
        PKTableScan tableScan = getTableScan();

        while(left.hasNext()){
            Tuple leftTuple = left.next();
            byte[] data = leftTuple.getContent(source).getData(foreignKey);
            BigInteger fk = Util.convertByteArrayToNumber(data);
            tableScan.setPrimaryKey(fk);
            tableScan.open();
            if(tableScan.hasNext()){
                Tuple rightTuple = tableScan.next();
                for (Map.Entry<String, RowData> entry:
                     rightTuple) {
                    leftTuple.setContent(entry.getKey(), entry.getValue());
                }
                return leftTuple;
            }else if(this.showIfNullRightOperator){
                return leftTuple;
            }
            tableScan.close();
        }
        return null;
    }

    protected PKTableScan getTableScan(){
        return (PKTableScan) right;
    }
}
