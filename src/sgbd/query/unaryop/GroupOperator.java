package sgbd.query.unaryop;

import engine.util.Util;
import sgbd.query.Operator;
import sgbd.query.Tuple;

import java.math.BigInteger;
import java.util.Comparator;

public class GroupOperator extends UnaryOperator {

    private String source,column;
    private UnaryOperator singleOperator;
    private PreGroupOperator preGroupOperator;
    private Tuple actualTuple;

    public GroupOperator(UnaryOperator singleOperator,String source, String column) {
        super(singleOperator);
        this.singleOperator=singleOperator;
        this.source = source;
        this.column = column;
    }

    @Override
    public void open() {
        Operator op = singleOperator.getOperator();
        preGroupOperator = new PreGroupOperator(op,source,column);
        singleOperator.setOperator(preGroupOperator);
        singleOperator.open();
    }

    private Tuple getNextTuple(){
        if(!preGroupOperator.isOpenned())return null;
        while(actualTuple == null) {
            if (operator.hasNext()) {
                actualTuple = operator.next();
            } else {
                operator.close();
                if(preGroupOperator.isOpenned()){
                    operator.open();
                }else{
                    return null;
                }
            }
        }
        return actualTuple;
    }

    @Override
    public Tuple next() {
        Tuple t= getNextTuple();
        actualTuple = null;
        return t;
    }

    @Override
    public boolean hasNext() {
        return getNextTuple()!=null;
    }

    @Override
    public void close() {
        if(preGroupOperator.isOpenned())
            operator.close();
    }

    class PreGroupOperator extends UnaryOperator{
        private BigInteger actualGroup;
        private Tuple actualTuple;
        private String source,column;
        private boolean openned;

        public PreGroupOperator(Operator operator,String source, String column) {
            super(new ExternalSortOperator(operator, source,column,false));
            this.source=source;
            this.column=column;
            this.openned = false;
        }

        protected boolean isOpenned(){
            return openned;
        }

        @Override
        public void open() {
            if(!openned){
                this.operator.open();
                actualTuple = this.operator.next();
                if(actualTuple==null)actualGroup=null;
                else{
                    actualGroup=Util.convertByteArrayToNumber(actualTuple.getContent(source).getData(column));
                }
            }else{
                if(actualTuple ==null){
                    actualTuple = this.operator.next();
                }
                if(actualTuple==null)actualGroup=null;
                else
                    actualGroup = Util.convertByteArrayToNumber(actualTuple.getContent(source).getData(column));
            }
        }


        private Tuple getTuple(){
            if(actualTuple==null){
                actualTuple = this.operator.next();
                if(actualTuple==null)return null;
            }
            if(actualGroup==null)return null;
            if(Util.convertByteArrayToNumber(actualTuple.getContent(source).getData(column)).compareTo(actualGroup)==0){
                return actualTuple;
            }else{
                return null;
            }
        }

        @Override
        public Tuple next() {
            Tuple t = getTuple();
            actualTuple = null;
            return t;
        }

        @Override
        public boolean hasNext() {
            Tuple t = getTuple();
            if(t!=null)return true;
            return false;
        }

        @Override
        public void close() {
            if(!this.operator.hasNext()){
                this.openned=false;
                this.operator.close();
            }
            this.actualGroup = null;
        }
    }
}
