/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lib.externalSort;

import sgbd.util.classes.TupleComparator;

import java.util.Arrays;

/**
 *
 * @author Sergio
 */
public class SortedOutputBucket extends OutputBucket{

    private TupleComparator comparator;

    public SortedOutputBucket(String folder, String name, int outputBucketSize,TupleComparator comparator){
        super(folder, name, outputBucketSize);
        this.comparator = comparator;
    }


    @Override
    public void saveBucket(){
    
        Arrays.sort(tuples,  0, currentIndex, comparator);
        
        super.saveBucket();
        
    }
    
}
