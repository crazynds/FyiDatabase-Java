/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lib.externalSort;

import engine.exceptions.DataBaseException;
import sgbd.info.ExternalSort;
import sgbd.query.Tuple;

import java.io.IOException;

/**
 *
 * @author Sergio
 */
public class OutputBucket {

    int bucketSize = 0;

    Tuple tuples[];
    int currentIndex = 0;

    TupleBucketIO tupleIO;

    
    boolean closed = false;

    public OutputBucket(String folder, String name, int bucketSize) {

        tupleIO = new TupleBucketIO(folder, name, true);
        this.bucketSize = bucketSize;
        tuples = new Tuple[bucketSize];
        try {
            tupleIO.open();
        } catch (Exception e) {
            throw new DataBaseException("OutputBucket->Constructor","Não foi possivel criar um TupleBucketIO");
        }
    }
    
    public void close(){
        try {
            tupleIO.close();
        } catch (IOException e) {
        }
        closed = true;
    }

    class MemoryBucket implements InputBucket {

        int currentReadIndex = 0;

        @Override
        public boolean hasNext() {
            if (currentReadIndex >= currentIndex) {
                return false;
            } else {
                return true;
            }
        }

        @Override
        public Tuple next()  {

            return tuples[currentReadIndex++];

        }

        @Override
        public void close() {
        }
    }

    public void addTuple(Tuple tuple) {
        
        if (closed)
            throw new DataBaseException("OutputBucket->saveBucket","file is closed");
        
        if (currentIndex >= bucketSize) {
            saveBucket();
        }
        tuples[currentIndex] = tuple;
        currentIndex++;
    }


    public boolean isFull() {
        return currentIndex == tuples.length;
    }

    public void saveBucket() {

        if (closed)
            throw new DataBaseException("OutputBucket->saveBucket","file is closed");
        
        for (int i = 0; i < currentIndex; i++) {
            Tuple tuple = tuples[i];
            tupleIO.saveTuple(tuple);
        }

        ExternalSort.TOTAL_NUMBER_TUPLES_WROTE += currentIndex;
        currentIndex = 0;
        try{
            tupleIO.updateFile(tuples[0]);
        } catch (Exception e) {
            throw new DataBaseException("OutputBucket->saveBucket","Não foi possivel sincronizar o bucket");
        }

    }

}
