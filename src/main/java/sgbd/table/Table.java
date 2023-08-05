package sgbd.table;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import engine.exceptions.DataBaseException;
import sgbd.index.Index;
import sgbd.prototype.TranslatorApi;
import sgbd.prototype.RowData;
import sgbd.table.components.Header;
import sgbd.table.components.RowIterator;

public abstract class Table implements Iterable<RowData>{

	protected TranslatorApi translatorApi;

	protected Header header;


	public Table(Header header)  {
		translatorApi =header.getPrototype().validateColumns();
		this.header = header;
	}



	public static Table openTable(Header header){
		return SimpleTable.openTable(header,false);
	}
	public static Table openTable(Header header, boolean clear){
		header.setBool("clear",clear);
		if(header.get(Header.TABLE_TYPE)==null)return new SimpleTable(header);
		switch (header.get(Header.TABLE_TYPE)){
			case "BTreeDoubleTable":
				return new BTreeDoubleTable(header);
			case "BTreeTable":
				return new BTreeTable(header);
			case "DoubleTable":
				return new DoubleTable(header);
			case "MemoryTable":
				return new MemoryTable(header);
			case "CSVTable":
				return new CSVTable(header,
						header.get("separator").charAt(0),
						header.get("delimiter").charAt(0),
						Integer.valueOf(header.get("beginIndex")));
			case "SimpleTable":
			default:
				return new SimpleTable(header);
		}
	}
	public static Table loadFromHeader(String headerPath){
		Header header;
		try {
			header = Header.load(headerPath);
		}catch (IOException ex){
			throw new DataBaseException("Table->saveHeader",ex.getMessage());
		}
		return openTable(header);
	}

	public Index createIndex(List<String> columns){
		return null;
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
		Retorna a classe responsavel por traduzir o byte array armazenado em uma linha de dados com diversas
		colunas. Util também para apontar como deve ser montado a chave primaria.
	 */
	public TranslatorApi getTranslator(){
		return translatorApi;
	}
	/*
		Retorna nome da table
	 */
	public String getTableName(){
		return header.get(Header.TABLE_NAME);
	}
	/*
		Retorna o objeto Header da table
	 */
	public Header getHeader(){
		return header;
	}

	/*
		Aceita apenas novos inserts, verifica chave primaria
	 */
	public abstract BigInteger insert(RowData r);
	public abstract void insert(List<RowData> r);
	public abstract RowData find(BigInteger pk);
	public abstract RowData find(BigInteger pk, List<String> colunas);
	//public abstract List<RowData> find(Query);
	/*
		Aceita apenas update para dados já existentes, se não encontrar gera um erro
	 */
	public abstract RowData update(BigInteger pk,RowData r);

	/*
		Retorna o dado deletado se ele existir
	 */
	public abstract RowData delete(BigInteger pk);

	/*
		Itera sobre os dados na tabela. Recebe como um dos parametros as colunas a serem lidas
	 */
	public abstract RowIterator iterator(List<String> columns);
	public abstract RowIterator iterator();





}
