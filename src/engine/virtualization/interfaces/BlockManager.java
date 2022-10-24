package engine.virtualization.interfaces;


public class BlockManager {

    private int lastBlock = 1;


    public void setNode(int node){
        if(lastBlock<=node)lastBlock=node+1;
    }

    public int allocNew(){
        return lastBlock++;
    }

    public void free(long block){

    }

    public void save(){

    }
}
