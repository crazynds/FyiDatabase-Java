/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lib.externalSort;

import engine.exceptions.DataBaseException;
import sgbd.prototype.query.Tuple;

import java.io.IOException;

/**
 *
 * @author Sergio
 */
public class FileInputBucket implements InputBucket {

    int totalNumberOfTuplesRead = 0;
    long currentNumberOfTuplesRead = 0;
    int currentIndex = -1;
    int bucketSize = -1;
    Tuple tuples[];

    TupleBucketIO bucketIO;
    
    boolean closed = false;

    public FileInputBucket(String folder, String name, int bucketSize) {

        bucketIO = new TupleBucketIO(folder, name, false);
        try{
            bucketIO.open();
            loadBucket();
        } catch (Exception e) {
            throw new DataBaseException("FileInputBucket->Constructor","Não foi possivel criar ou carregar o TupleBucketIO");
        }
        this.bucketSize = bucketSize;
        tuples = new Tuple[bucketSize];
    }

    @Override
    public boolean hasNext() {
        if (closed)
            throw new DataBaseException("OutputBucket->hasNext","file is closed");
        
        if (totalNumberOfTuplesRead >= bucketIO.getBucketSize()) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public Tuple next() {
        if (closed)
            throw new DataBaseException("OutputBucket->next","file is closed");
        
        if (currentIndex + 1 < currentNumberOfTuplesRead) {
            totalNumberOfTuplesRead++;
            return tuples[++currentIndex];
        } else {
            try {
                loadBucket();
            } catch (Exception e) {
                throw new DataBaseException("OutputBucket->next","Error when reading bucket");
            }
            totalNumberOfTuplesRead++;
            return tuples[++currentIndex];
        }

    }

    @Override
    public void close(){
        try {
            bucketIO.close();
        } catch (IOException e) {
        }
        closed = true;
    }
    
    private void loadBucket() throws Exception {

        currentNumberOfTuplesRead = bucketSize;
        if (totalNumberOfTuplesRead + currentNumberOfTuplesRead > bucketIO.getBucketSize()) {
            currentNumberOfTuplesRead = bucketIO.getBucketSize() - totalNumberOfTuplesRead;
        }
        for (int i = 0; i < currentNumberOfTuplesRead; i++) {
            tuples[i] = bucketIO.readTuple();

        }
        currentIndex = -1;
    }

}
