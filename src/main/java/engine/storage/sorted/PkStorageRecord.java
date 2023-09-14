package engine.storage.sorted;

import engine.storage.StorageRecord;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordStream;

import java.util.List;
import java.util.Map;

public abstract class PkStorageRecord<T> extends StorageRecord<T> {

    /*
     * Esse tipo de classe é especificado para as estruturas de armazenamento que no qual a classe
     * pai que controla a chave do record, e essa chave deve sempre ser passada quando se deseja
     * mexer em um record especifico
     */
    public PkStorageRecord(){

    }


    /*
     * Essa função tem como objetivo procurar algum record que tenha a chave correspondente
     * e atualiza-la com as informações do record correspondente.
     * Caso não encontre ela deve ser adicionada ao armazenamento.
     */
    public abstract void write(T key, Record r);
    public abstract void write(List<Map.Entry<T,Record>> list) ;

    public abstract void delete(T key);


}
