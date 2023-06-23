package sgbd.query.agregation;

import sgbd.prototype.column.Column;
import sgbd.prototype.ComplexRowData;
import sgbd.prototype.query.Tuple;
import sgbd.util.global.Util;

public class AvgAgregation extends AgregationOperation{

    private final String uniqueSourceName;

    public AvgAgregation(String sourceSrc, String columnSrc, String sourceDst, String columnDst) {
        super(sourceSrc, columnSrc, sourceDst, columnDst);
        uniqueSourceName = "__temp_avg_"+hashCode();
    }

    public AvgAgregation(String sourceSrc, String columnSrc) {
        super(sourceSrc, columnSrc);
        uniqueSourceName = "__temp_avg_"+hashCode();
    }


    @Override
    public String getAgregationName() {
        return "avg";
    }

    @Override
    public void initialize(Tuple accumulator) {
        ComplexRowData row = accumulator.getContent(uniqueSourceName);
        row.setDouble("sum",0);
        row.setLong("qtd",0);
    }

    @Override
    public void process(Tuple accumulator, Tuple newData){
        ComplexRowData row = accumulator.getContent(uniqueSourceName);
        double sum = row.getDouble("sum");
        long qtd = row.getLong("qtd");
        Column meta = newData.getContent(sourceSrc).getMeta(columnSrc);
        if(meta==null)return;
        switch (Util.typeOfColumn(meta)){
            case "int":
                sum += newData.getContent(sourceSrc).getInt(columnSrc);
                break;
            case "long":
                sum += newData.getContent(sourceSrc).getLong(columnSrc);
                break;
            case "double":
                sum += newData.getContent(sourceSrc).getDouble(columnSrc);
                break;
            case "float":
                sum += newData.getContent(sourceSrc).getFloat(columnSrc);
                break;
            case "string":
                sum += newData.getContent(sourceSrc).getString(columnSrc).length();
                break;
            default:
                sum += 0;
        }
        qtd++;
        row.setDouble("sum",sum);
        row.setLong("qtd",qtd);
    }

    @Override
    public void finalize(Tuple accumulator) {
        ComplexRowData row = accumulator.getContent(uniqueSourceName);
        double sum = row.getDouble("sum");
        long qtd = row.getLong("qtd");
        double result = (qtd>0)?(sum / qtd):0;

        accumulator.getContent(sourceDst).setDouble(columnDst,result,new Column("avg",(short)8,Column.FLOATING_POINT));
        accumulator.setContent(uniqueSourceName,null);
    }

}
