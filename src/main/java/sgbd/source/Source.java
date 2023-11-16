package sgbd.source;

import engine.exceptions.DataBaseException;
import sgbd.prototype.RowData;
import sgbd.prototype.TranslatorApi;
import sgbd.source.components.Header;
import sgbd.source.components.RowIterator;

import java.io.IOException;
import java.util.List;

public abstract class Source<T> implements Iterable<RowData>{

    protected TranslatorApi translatorApi;

    protected Header header;


    public Source(Header header)  {
        if (header.getPrototype() != null) {
            translatorApi = header.getPrototype().validateColumns();
        }

        this.header = header;
    }

    /*
        Abre e fecha as propriedades da tabela
        Abre e fecha o acesso ao arquivo
     */
    public abstract void clear();
    public abstract void open();
    public abstract void close();
    public void saveHeader(String path) {
        try {
            header.save(path);
        }catch (IOException ex){
            throw new DataBaseException("Table->saveHeader",ex.getMessage());
        }
    }

    /*
        Retorna o objeto Header da table
     */
    public Header getHeader(){
        return header;
    }

    /*
        Retorna a classe responsavel por traduzir o byte array armazenado em uma linha de dados com diversas
        colunas. Util também para apontar como deve ser montado a chave primaria.
     */
    public TranslatorApi getTranslator(){
        return translatorApi;
    }



    public abstract RowData findByRef(T reference);


    /*
        Itera sobre os dados na fonte. Recebe como um dos parametros as colunas a serem lidas
     */
    protected abstract RowIterator<T> iterator(List<String> columns,T lowerbound);
    public abstract RowIterator<T> iterator(List<String> columns);
    public abstract RowIterator<T> iterator();

    /*
        Retorna nome da fonte
     */
    public abstract String getSourceName();

}
