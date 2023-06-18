package sgbd.query.unaryop;

import sgbd.query.Operator;
import sgbd.query.Tuple;

import java.util.ArrayList;
import java.util.List;

public class SelectColumnsOperator extends UnaryOperator{
    private List<String[]> srcColumns;

    public SelectColumnsOperator(Operator op, List<String> srcColumns) {
        super(op);
        ArrayList<String[]> arr = new ArrayList<>();
        for (String s:
                srcColumns) {
            String[] vals = s.split("\\.");
            String[] v = {vals[0],vals[1]};
            arr.add(v);
        }
        this.srcColumns = arr;
    }

    @Override
    public Tuple next() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return false;
    }
}
