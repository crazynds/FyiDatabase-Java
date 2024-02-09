/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.index.btree;

import ibd.persistent.ExternalizablePage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lib.BigKey;

/**
 *
 * @author Sergio
 */
public class Key implements Comparable<Key>, ExternalizablePage {

    //A key is composed by a number of comparable objects
    private Comparable[] keys;

    //the datatype of the objects that compose keys are define in the schema
    private RowSchema schema;

    public Key(RowSchema schema) {
        keys = new Comparable[schema.getSize()];
        this.schema = schema;
    }

    /*
    * sets all objects that are part of a key
    * this function does not verify if the objects conform with the expected datatype, as described in the schema
     */
    public void setKeys(Comparable[] keys) {
        this.keys = keys;
    }

    public Object get(int index) {
        return keys[index];
    }

    /*
    * returns a string representation of the key considering the concatenatin of its first parts
     */
    public String getPartialKey(int parts) {
        String result = "";
        for (int i = 0; i < parts; i++) {
            result += keys[i] + ",";
        }

        return result;

    }

    @Override
    public boolean equals(Object o) {
        Key k1 = (Key) o;
        //return Arrays.equals(keys, k1.keys);

        int res = this.compareTo(k1);
        return (res == 0);
    }

    
    /**
     * This is a method that allows comparisons to take place between
     * DictionaryPair objects in order to sort them later on
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(Key other) {
        Key key1 = (Key) other;
        
        int minLength = Math.min(keys.length, key1.keys.length);

        for (int i = 0; i < minLength; i++) {
            int res = (keys[i].compareTo(key1.keys[i]));
            if (res != 0) {
                return res;
            }
        }
        // If the elements are equal, continue to the next position

        // If all the elements up to minLength are equal, the longer array is considered higher
        if (keys.length > key1.keys.length) {
            return 1;
        } else if (keys.length < key1.keys.length) {
            return -1;
        } else {
            return 0;
        }
        
    }
    
    
    /*
    returns true if the first levels of this key are equal to some other key
    */
    //@Override
    public boolean match(Key otherKey) {
        //if the number of existing levels in lower than the number of levels to be compared
        int minLength = Math.min(keys.length, otherKey.keys.length);

        for (int i = 0; i < minLength; i++) {
            int res = (keys[i].compareTo(otherKey.keys[i]));
            if (res != 0) {
                return false;
            }
        }
        
        
        //all first levels have equal values
        return true;
    }

    @Override
    public String toString() {
        return getPartialKey(keys.length);
    }

    /*
    * the size refers to the amount of bytes taken by each object of the key.
     */
    @Override
    public int getSizeInBytes() {
        int size = 0;
        for (int i = 0; i < keys.length; i++) {
            size += schema.getDataSizeInBytes(i, keys[i]);
        }

        return size;
    }

    @Override
    public void writeExternal(DataOutput out) throws IOException {
        //out.writeInt(keys.length);
        for (int i = 0; i < keys.length; i++) {
            switch (schema.get(i)) {
                case 'I':
                    out.writeInt((Integer) keys[i]);
                    continue;
                case 'L':
                    out.writeLong((Long) keys[i]);
                    continue;
                case 'S':
                    out.writeUTF((String) keys[i]);
                    continue;
                case 'K':
                    out.write(((BigKey)keys[i]).getData());
            }
        }
    }

    @Override
    public void readExternal(DataInput in) throws IOException {
        for (int i = 0; i < keys.length; i++) {
            switch (schema.get(i)) {
                case 'I':
                    keys[i] = in.readInt();
                    continue;
                case 'L':
                    keys[i] = in.readLong();
                    continue;
                case 'S':
                    keys[i] = in.readUTF();
                    continue;
                case 'K':
                    byte b[] = new byte[schema.getSize(i)];
                    in.readFully(b);
                    keys[i] = new BigKey(b);
            }
        }
    }

    
    

}
