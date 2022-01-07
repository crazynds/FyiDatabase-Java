package engine.virtualization.record;

import engine.file.streams.ReadByteStream;

import java.math.BigInteger;

public interface RecordInterface {

	public BigInteger getPrimaryKey(Record r);
	public BigInteger getPrimaryKey(ReadByteStream rbs);

	public boolean isActiveRecord(Record r);
	public boolean isActiveRecord(ReadByteStream rbs);
	
	public void setActiveRecord(Record r,boolean active);
	
	/*
	 * Atualiza a referencia da primary key na tabela de referencias do manipulador;
	 * Não é ncessário a implementação de uma lógica dentro dessa função
	 */
	public void updeteReference(BigInteger pk,long key);
	
}
