package sgbd.query.agregation;

import sgbd.prototype.Column;
import sgbd.query.Tuple;

public class CountAgregation extends AgregationOperation{
    public CountAgregation(String sourceSrc, String columnSrc, String sourceDst, String columnDst) {
        super(sourceSrc, columnSrc, sourceDst, columnDst);
    }

    public CountAgregation(String sourceSrc, String columnSrc) {
        super(sourceSrc, columnSrc);
    }

    @Override
    public String getAgregationName() {
        return "count";
    }

    @Override
    public void initialize(Tuple acumulator) {
        acumulator.getContent(sourceDst).setLong(columnDst,0, new Column("count", (short) 8,Column.NONE));
    }

    @Override
    public void process(Tuple acumulator, Tuple newData) {
        if(sourceSrc=="*"
                || (newData.getContent(sourceSrc).size()>0 && columnSrc=="*")
                || newData.getContent(sourceSrc).getData(columnSrc)!=null){
            Long acc = acumulator.getContent(sourceDst).getLong(columnDst);
            acumulator.getContent(sourceDst).setLong(columnDst,acc+1);
        }
    }

    @Override
    public void finalize(Tuple acumulator) {
        // Do nothing
    }
}
