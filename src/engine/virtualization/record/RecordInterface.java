package engine.virtualization.record;

import java.math.BigInteger;

public interface RecordInterface {

	public BigInteger getPrimaryKey(Record r) ;

	public boolean isActiveRecord(Record r);
	
	public void setActiveRecord(Record r,boolean active);
	
	/*
	 * Atualiza a referencia da primary key na tabela de referencias do manipulador;
	 */
	public void updeteReference(BigInteger pk,long key);
	
}
