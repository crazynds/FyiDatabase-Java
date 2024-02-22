package sgbd.source;

import engine.exceptions.DataBaseException;
import lib.BigKey;
import sgbd.prototype.RowData;
import sgbd.prototype.TranslatorApi;
import sgbd.source.components.Header;
import sgbd.source.components.RowIterator;

import java.io.IOException;
import java.util.List;

public abstract class Source implements Iterable<RowData>{

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


    /*
        Realiza a inserção, verifica chave primaria e substitui se já existir
     */
    public abstract void insert(RowData r);
    public void insert(List<RowData> r){
        for (RowData row: r) {
            this.insert(row);
        }
    }


    /*
        Itera sobre os dados na fonte. Recebe como um dos parametros as colunas a serem lidas
     */
    public abstract RowData findByRef(RowData reference);

    protected abstract RowIterator iterator(List<String> columns,RowData lowerbound);
    public abstract RowIterator iterator(List<String> columns);
    public abstract RowIterator iterator();

    /*
        Retorna nome da fonte
     */
    public abstract String getSourceName();

}
