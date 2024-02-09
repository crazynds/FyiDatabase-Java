/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.index.btree;

import java.util.ArrayList;


public abstract class BPlusTree {

    //the order of the internal nodes
    int m;
    //the order of the leaf nodes
    int leafM;
    
    //the schema of the indexed keys
    RowSchema keySchema;
    
    //the schema of the indexed values
    RowSchema valueSchema;

    /**
     * Constructor
     *
     * @param m: the order (fanout) of the B+ tree internal nodes
     * @param leafM: the order (fanout) of the B+ tree leaf nodes
     * @param keySchema: the schema of the indexed keys
     * @param valueSchema: the schema of the indexed values
     */
    public BPlusTree(int m, int leafM, RowSchema keySchema, RowSchema valueSchema) {
        this.m = m;
        this.leafM = leafM;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        
    }
    
    public RowSchema getKeySchema() {
        return keySchema;
    }
    
    public RowSchema getValueSchema() {
        return valueSchema;
    }
    
    
    /**
     * Given a key, this method will remove the dictionary pair with the
     * corresponding key from the B+ tree.
     *
     * @param key: a value that corresponds with an existing dictionary pair
     * @return the value associated with the key or null if it does not exist
     */
    public abstract Value delete(Key key);
    
    /**
     * Given a key and a value, this method inserts a
     * dictionary pair accordingly into the B+ tree.
     *
     * @param key: a key to be used in the dictionary pair
     * @param value: a value to be used in the dictionary pair
     */
    public abstract boolean insert(Key key, Value value);
    
    /**
     * Given a key, this method returns the value associated with the key within
     * a dictionary pair that exists inside the B+ tree.
     *
     * @param key: the key to be searched within the B+ tree
     * @return the value associated with the key within the B+
     * tree
     */
    public abstract Value search(Key key);
    

    /**
     * This method traverses the doubly linked list of the B+ tree leaf nodes and finds
     * all values whose associated keys are within the range specified by
     * lowerBound and upperBound.
     *
     * @param lowerBound: (int) the lower bound of the range
     * @param upperBound: (int) the upper bound of the range
     * @return an ArrayList<Value> that holds all values of dictionary pairs
     * whose keys satisfy are search conditions.
     */
    public abstract ArrayList<Value> search(Key lowerBound, Key upperBound);

    /**
     * This method traverses the doubly linked list of the B+ tree leaf nodes and finds
     * all values.
     * @return an ArrayList<DictionaryPair> that holds all dictionary pairs
     * 
     */
    public abstract ArrayList<DictionaryPair> searchAll();
    
    /**
     * This method traverses the doubly linked list of the B+ tree leaf nodes and finds
     * all values whose associated keys have a partial match with a given key.
     *
     * @param key: (Key) the key to be compared with
     * @return an ArrayList<Value> that holds all values of dictionary pairs
     * whose key satisfy the search condition.
     */
    public abstract ArrayList<Value> partialSearch(Key key);

    public abstract ArrayList<DictionaryPair> partialSearchDP(Key key);
    
    /**
     * This method updates the value of the DictionaryPair that holds a given key.
     *
     * @param key: the key to be searched within the B+ tree
     * @param value: the value to replace the current indexed value
     * @return the value updated, or null if the DictionaryPair was not found
     */
    public abstract Value update(Key key, Value value);
    
    

    
}
