package engine.storage.common;

import engine.storage.StorageRecord;
import engine.virtualization.interfaces.StorageEventHandler;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordStream;

import java.util.List;

public abstract class AnonymousStorageRecord extends StorageRecord<Long>{

    protected final StorageEventHandler handler;

    /*
     * Esse tipo de classe é especificado para as estruturas de armazenamento que no qual o Storage
     * que vai controlar a chave do record, sempre que essa chave for atualizar ele vai emitir
     * um evento no qual a classe pai deve tratar corretamente.
     */
    public AnonymousStorageRecord(StorageEventHandler handler){
        this.handler = handler;
    }

    /*
     * Essa função vai adicionar o record ao armazenamento
     */
    public abstract void write(Record r);
    public abstract void write(List<Record> list) ;


    /*
     * Essa função vai atualizar o record na chave passada
     */
    public abstract void update(long key,Record r);

    /*
     * Essa função vai delete o record do armazenamento
     * Ela vai mover outros records se necessário, e vai chamar o handler para atualizar os eventos
     */
    public abstract void delete(long key);


}
