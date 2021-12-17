package sgbd.table;

import java.math.BigInteger;
import java.util.List;

import sgbd.prototype.Prototype;
import sgbd.prototype.TranslatorApi;
import sgbd.prototype.RowData;

public abstract class Table implements Iterable<RowData>{

	protected TranslatorApi translatorApi;
	
	public Table(Prototype pt)  {
		translatorApi =pt.validateColumns();
	}
	/*
		Abre e fecha as propriedades da tabela
	 */
	public abstract void open();
	public abstract void close();

	/*
		Retorna a classe responsavel por traduzir o byte array armazenado em uma linha de dados com diversas
		colunas. Util também para apontar como deve ser montado a chave primaria.
	 */
	public TranslatorApi getTranslator(){
		return translatorApi;
	}


	/*
		Aceita apenas novos insertes, verifica chave primaria
	 */
	public abstract BigInteger insert(RowData r);
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
}
