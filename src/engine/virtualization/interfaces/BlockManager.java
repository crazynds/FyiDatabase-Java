package engine.virtualization.interfaces;


public class BlockManager {

    private int lastBlock = 0;


    public int allocNew(){
        return lastBlock++;
    }

    public void free(long block){

    }

    public void save(){

    }
}
