package engine.virtualization.record.manager.storage;

import engine.virtualization.record.Record;
import engine.virtualization.record.RecordStream;

import java.math.BigInteger;
import java.util.List;

public interface RecordStorageController {

	/*
	 *	O RecordStorage organiza os records baseado em posições, ou seja, a key passada
	 * é relativa a posição que aquele storage está armazenado sendo 0 o primeiro dado
	 * e N-1 o ultimo dado.
	 *
	 * 	O RecordStorage também implementa
	 */

	/*
	 * Inicia um arquivo do zero
	 * Reinicia todos os dados necessários
	 */
	public void restartFileSet() ;

	/*
	 * Força os buffers a liberarem as modificações escritas
	 */
	public void flush() ;

	/*
	 * Le o dado a partir da posição solicitada
	 */
	public Record read(long key) ;
	public void read(long key,byte[] buffer) ;

	/*
	 * Sobrescreve o record naquela posição
	 */
	public long write(Record r,long key);
	
	/*
	 * Escreve um record na posição anterior aquela
	 */
	public long writeNew(Record r);
	public void writeNew(List<Record> list);

	/*
	 * Faz uma busca de um dado pela primary key utilizando o melhor algoritmo daquela
	 * estrutura
	 */
	public boolean search(BigInteger pk, byte[] buffer);

	/*
	 * Retorna um objeto para efetuar a leitura sequencial
	 */
	public RecordStream sequencialRead();
}
