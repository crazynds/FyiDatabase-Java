package sgbd.query.agregation;

import sgbd.query.Tuple;

public abstract class AgregationOperation {


    protected String sourceSrc,columnSrc, sourceDst, columnDst;
    public AgregationOperation(String sourceSrc, String columnSrc, String sourceDst, String columnDst){
        this.sourceSrc = sourceSrc;
        this.columnSrc = columnSrc;
        this.sourceDst = sourceDst;
        this.columnDst = columnDst;
    }

    public AgregationOperation(String sourceSrc, String columnSrc){
        this.sourceSrc = sourceSrc;
        this.columnSrc = columnSrc;
        this.sourceDst = sourceSrc;
        this.columnDst = getAgregationName()+'('+columnSrc+')';
    }

    public abstract String getAgregationName();

    public abstract void initialize(Tuple acumulator);
    public abstract void process(Tuple acumulator, Tuple newData);
    public abstract void finalize(Tuple acumulator);

}
