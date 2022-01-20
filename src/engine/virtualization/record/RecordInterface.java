package engine.virtualization.record;

import engine.file.streams.ReadByteStream;

import java.math.BigInteger;

public interface RecordInterface extends RecordInfoExtraction {
	
	/*
	 * Atualiza a referencia da primary key na tabela de referencias do manipulador;
	 * Não é ncessário a implementação de uma lógica dentro dessa função
	 */
	public void updeteReference(BigInteger pk,long key);
	
}
