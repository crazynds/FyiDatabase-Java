/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.index.btree;

import ibd.persistent.PageFile;

public class bplusFarFile  extends BPlusTreeFile{

    public bplusFarFile(int m, int leafM, PageFile pagefile,  RowSchema keyPrototype, RowSchema valuePrototype) throws InstantiationException, IllegalAccessException {
        super(m, leafM, pagefile,  keyPrototype, valuePrototype);
    }

    
    
    /**
     * Creates a header for this index structure which is an instance of
     * {@link TreeIndexHeader}. Subclasses may need to overwrite this method if
     * they need a more specialized header.
     *
     * @return a new header for this index structure
     */
    protected TreeIndexHeader createHeader(int pageSize, int dirCapacity, int leafCapacity, int rootID, int firstLeafID, RowSchema keySchema, RowSchema valueSchema){
        return new BTreeIndexHeader(pageSize, dirCapacity, leafCapacity, rootID, firstLeafID, keySchema, valueSchema, 100);
    }
    

    public void setOuterSeqLevel(int level, int value){
        ((BTreeIndexHeader) file.getHeader()).setOuterSeqLevel(level, value); 
  }
  
  public int getOuterSeqLevel(int level){
      return ((BTreeIndexHeader) file.getHeader()).getOuterSeqLevel(level);
  }
    
}
