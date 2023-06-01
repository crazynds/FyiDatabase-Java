/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lib.externalSort;

import sgbd.query.Tuple;

/**
 *
 * @author Sergio
 */
public interface InputBucket {

    public boolean hasNext();

    public Tuple next();
    
    public void close();
     

}
