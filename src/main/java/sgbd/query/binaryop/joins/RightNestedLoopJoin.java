package sgbd.query.binaryop.joins;

import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;
import sgbd.table.Table;
import sgbd.util.interfaces.ComparableFilter;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RightNestedLoopJoin extends LeftNestedLoopJoin{

    @Deprecated
    public RightNestedLoopJoin(Operator left, Operator right, ComparableFilter<Tuple> comparator) {
        super(right, left, (t1, t2) -> comparator.match(t2,t1)); // apenas inverte os operadores
    }

    public List<Table> getSources(){
        List<Table> rTable = right.getSources();
        List<Table> lTable = left.getSources();
        return Stream.concat(rTable.stream(),lTable.stream()).collect(Collectors.toList());
    }
    @Override
    public Map<String, List<String>> getContentInfo() {
        return getStringListMap(right, left);
    }
}
