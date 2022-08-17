package engine.virtualization.interfaces;


public class BlockManager {

    private int lastBlock = 0;


    public long allocNew(){
        return lastBlock++;
    }

    public void free(long block){

    }

    public void save(){

    }
}
