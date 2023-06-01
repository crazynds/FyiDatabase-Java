/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lib.externalSort;


import engine.exceptions.DataBaseException;
import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.query.unaryop.UnaryOperator;
import sgbd.util.classes.ResourceName;
import sgbd.util.classes.TupleComparator;

import java.util.List;
import java.util.Map;

/**
 *
 * @author Sergio
 */
public class ExternalSort extends UnaryOperator {

    ExternalMergeSort sorter;
    InputBucket sortedInputBucket;
    
    int inputBucketSize;
    int outputBucketSize;

    ResourceName resource;
    
    public ExternalSort(Operator op) throws Exception{
        this(op, null, 10, 10);
    }
    
    public ExternalSort(Operator op, ResourceName resource, int inputBucketSize, int outputBucketSize) throws Exception{
        super(op);
        this.resource = resource;
        this.inputBucketSize = inputBucketSize;
        this.outputBucketSize = outputBucketSize;
        findSourceIndex();
    }
    
    private void findSourceIndex() throws Exception{
        Map<String, List<String>> sources = getContentInfo();
        if (resource == null) {
            for (Map.Entry<String,List<String>> source:sources.entrySet()) {
                for(String column:source.getValue()){
                    resource = new ResourceName(source.getKey(),column);
                    break;
                }
            }
        } else {
            boolean finded = false;
            if(sources.get(resource.getSource()) != null){
                for(String column:sources.get(resource.getSource())){
                    if(column.compareTo(resource.getColumn())==0){
                        finded = true;
                        break;
                    }
                }
            }
            if(!finded)throw new DataBaseException("ExternalSort->Constructor","Recurso solicitado não foi encontrado ("+resource.toString()+")");
        }
        if (resource == null)
            throw new DataBaseException("ExternalSort->Constructor","Nenhum recurso disponivel");
    }
    
    
    @Override
    public void open() {
        operator.open();
        if(sorter!=null)return;

        sorter = new ExternalMergeSort(inputBucketSize, outputBucketSize,new TupleComparator(resource));

        while (operator.hasNext()) {
            Tuple tuple = operator.next();
            sorter.addTuple(tuple);
        }
        sortedInputBucket = sorter.sort();
        
    }
    
    
    @Override
    public boolean hasNext(){
        if(sortedInputBucket==null)return false;
        return sortedInputBucket.hasNext();
    }


    @Override
    public Tuple next(){
        if(sortedInputBucket==null)return null;
        return sortedInputBucket.next();
    }


    @Override
    public void close() {
        operator.close();
        sorter = null;
        sortedInputBucket = null;
    }

}
