package sgbd.table;

import java.math.BigInteger;

import engine.file.FileManager;
import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;

public abstract class Table implements Iterable<RowData>{

	private Prototype columns;
	
	public Table(Prototype pt)  {
		pt.validateColumns();
		this.columns=pt;
	}
	/*
		Abre e fecha as propriedades da tabela
	 */
	public abstract void open();
	public abstract void close();
	
	/*
		Aceita apenas novos insertes, verifica chave primaria
	 */
	public abstract void insert(RowData r);
	public abstract RowData find(BigInteger pk);
	//public abstract RowData find(Query);
	/*
		Aceita apenas update para dados já existentes, se não encontrar gera um erro
	 */
	public abstract void update(RowData r);

	/*
		Retorna o dado deletado se ele existir
	 */
	public abstract RowData delete(BigInteger pk);
}
