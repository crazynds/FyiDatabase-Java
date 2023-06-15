package sgbd.util.classes;

import sgbd.prototype.BData;
import sgbd.prototype.Column;
import sgbd.util.statitcs.Util;

public class AnonymousDataType {

    private String type;

    public AnonymousDataType(Column dataType){
        type = Util.typeOfColumn(dataType);
    }


    public BData sum(BData a, BData b){


        return null;

    }
    public BData sub(BData a, BData b){

        return null;
    }
    public BData mul(BData a, BData b){

        return null;
    }
    public BData div(BData a, BData b){

        return null;
    }

    public int compare(BData a, BData b){

        return 0;
    }


}
