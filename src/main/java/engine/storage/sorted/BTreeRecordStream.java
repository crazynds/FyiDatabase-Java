/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package engine.storage.sorted;

import engine.virtualization.record.Record;
import engine.virtualization.record.RecordStream;
import ibd.index.btree.DictionaryPair;
import java.util.Iterator;
import java.util.List;
import lib.BigKey;

/**
 *
 * @author ferna
 */
public class BTreeRecordStream implements RecordStream<BigKey>{

    List<DictionaryPair> pairs;
    Iterator<DictionaryPair> iterator;
    
    public BTreeRecordStream(List<DictionaryPair> pairs){
        this.pairs = pairs;
        
    }
    
    @Override
    public void open() {
        iterator = pairs.iterator();
    }

    @Override
    public void close() {
    }

    @Override
    public BigKey getKey() {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    public Record getRecord() {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    public void update(Record r) {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    public void reset() {
        this.iterator = pairs.iterator();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Record next() {
        DictionaryPair pair = iterator.next();
        return (Record)(pair.getValue().get(0));
    }
    
}
