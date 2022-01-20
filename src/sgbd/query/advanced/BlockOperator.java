package sgbd.query.advanced;

import sgbd.query.basic.Operator;

public interface BlockOperator {

    public void open();

    /*
        Block pointer
     */
    public int getBlock();
    public void setBlock(int block);


    /*
        Next and hasNext based on blocks
        Each operator is a block of dataset
     */
    public Operator next();
    public boolean hasNext();

    public void close();
}
