/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.index.btree;

import engine.virtualization.record.instances.GenericRecord;
import ibd.persistent.ExternalizablePage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 *
 * @author Sergio
 */
public class Value implements ExternalizablePage {

    //A value is composed by a number of objects
    private Object[] objects;

    //the datatype of the objects that compose a value are define in the schema
    private RowSchema schema;

    public Value(RowSchema prototype) {
        objects = new Object[prototype.getSize()];
        this.schema = prototype;
    }

    public Object get(int index) {
        return objects[index];
    }

    /*
    * sets an object to a specific posistion of the obejcts array
    * this function does not verify if the object conform with the expected datatype, as described in the schema
     */
    public void set(int index, Object value) {
        objects[index] = value;
    }

    public int size() {
        return objects.length;
    }

//    public Integer getInt(int index) {
//        return (Integer) array[index];
//    }
//
//    public void setInt(int index, int value) {
//        array[index] = value;
//    }
//
//    public String getString(int index) {
//        return (String) array[index];
//    }
//
//    public void setString(int index, String value) {
//        array[index] = value;
//    }
    // Add more specific getter and setter methods for other types as needed
//    public String getObject(){
//        return object;
//    }
    @Override
    public String toString() {
        return Arrays.toString(objects);
    }

    /*
    * the size refers to the amount of bytes taken by each object of the value.
     */
    @Override
    public int getSizeInBytes() {
        int size = 0;
        for (int i = 0; i < objects.length; i++) {
            size += schema.getDataSizeInBytes(i, objects[i]);
        }

        return size;
    }

    @Override
    public void writeExternal(DataOutput out) throws IOException {
        for (int i = 0; i < objects.length; i++) {
            switch (schema.get(i)) {
                case 'I':
                    out.writeInt((Integer) objects[i]);
                    continue;
                case 'L':
                    out.writeLong((Long) objects[i]);
                    continue;
                case 'S':
                    out.writeUTF((String) objects[i]);
                    continue;
                case 'R':
                    byte target[] = new byte[schema.getSize(i)];
                    byte source[] = ((GenericRecord)objects[i]).getData();
                    System.arraycopy(source, 0, target, 0, source.length);
                    out.write(target);
            }
        }
    }

    @Override
    public void readExternal(DataInput in) throws IOException {
        for (int i = 0; i < objects.length; i++) {
            switch (schema.get(i)) {
                case 'I':
                    objects[i] = in.readInt();
                    continue;
                case 'L':
                    objects[i] = in.readLong();
                    continue;
                case 'S':
                    objects[i] = in.readUTF();
                    continue;
                case 'R':
                    byte b[] = new byte[schema.getSize(i)];
                    in.readFully(b);
                    objects[i] = new GenericRecord(b);
            }
        }
    }
}
