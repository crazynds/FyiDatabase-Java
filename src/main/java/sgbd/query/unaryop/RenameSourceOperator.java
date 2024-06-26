package sgbd.query.unaryop;

import lib.booleanexpression.entities.AttributeFilters;
import sgbd.prototype.RowData;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;

import java.util.List;
import java.util.Map;

public class RenameSourceOperator extends SimpleUnaryOperator{

    private String sourceSrc,sourceDst;

    public RenameSourceOperator(Operator op,String sourceSrc,String sourceDst) {
        super(op);
        this.sourceSrc = sourceSrc;
        this.sourceDst = sourceDst;
    }

    @Override
    public void lookup(AttributeFilters filters) {

        super.lookup(filters);
    }

    @Override
    public Tuple getNextTuple() {
        if(!operator.hasNext())
            return null;
        Tuple t = operator.next();
        RowData row = t.getContent(this.sourceSrc);
        t.setContent(this.sourceDst,row);
        t.setContent(this.sourceSrc,null);
        return t;
    }

    @Override
    public Map<String, List<String>> getContentInfo() {
        Map<String,List<String>> map= super.getContentInfo();
        List<String> list = map.remove(this.sourceSrc);
        map.put(this.sourceDst,list);
        return map;
    }
}
