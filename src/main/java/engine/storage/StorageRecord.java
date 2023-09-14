package engine.storage;

import engine.virtualization.record.RecordStream;

public abstract class StorageRecord<T> {

    /*
     * Inicia um arquivo do zero
     * Reinicia todos os dados necessários
     */
    public abstract void restart() ;

    /*
     * Força os buffers a liberarem as modificações escritas
     */
    public abstract void flush();

    /*
     * Fecha a manipulação do arquivo e faz o salvamento dos dados
     */
    public abstract void close();

    /*
     * Abre uma stream a partir do valor da chave passada, se não encontrado,
     * vai abrir no registro com o menor valor que é maior que a chave.
     *
     * Uma chave zero representa uma leitura desde o inicio.
     */
    public abstract RecordStream<T> read(T key);

}
