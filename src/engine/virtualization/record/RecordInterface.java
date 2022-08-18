package engine.virtualization.record;

import java.math.BigInteger;

public class RecordInterface{

	private RecordInfoExtractor infoExtraction;
	public RecordInterface(RecordInfoExtractor extraction){
		this.infoExtraction=extraction;
	}

	/*
	 * Atualiza a referencia da primary key na tabela de referencias do manipulador;
	 * Não é ncessário a implementação de uma lógica dentro dessa função
	 * mas pode ser utilizada por otmizadores e cache para busca futura
	 */
	public void updeteReference(BigInteger pk,long key){

	}

	public RecordInfoExtractor getExtractor(){
		return infoExtraction;
	}

}
