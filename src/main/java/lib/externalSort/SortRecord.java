/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lib.externalSort;

import ibd.table.record.AbstractRecord;

/**
 *
 * @author Sergio
 */
public class SortRecord extends AbstractRecord{

    String content;
    Long primaryKey; 
    
    @Override
    public Integer getRecordId() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getContent() {
        return content;
    }
    
    public void setContent(String c) {
        content = c;
    }

    @Override
    public Integer getBlockId() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Long getPrimaryKey() {
        return primaryKey;
    }
    
    public void setPrimaryKey(Long pk) {
        primaryKey = pk;
    }
}
