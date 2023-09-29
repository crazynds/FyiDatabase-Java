package engine.virtualization.record;



import java.util.Iterator;

public interface RecordStream<T> extends Iterator<Record> {
	
	/*
	 * Abre e fecha o leitor sequencial
	 * É importante caso seja necessário bloquear a tabela dependendo do record manager
	 */
	public void open();
	public void close();

	/*
	 * Retorna a chave da posição atual
	 */
	public T getKey();

	/*
	 * Faz a leitura do record ou a posição em que ele está armazendo no banco de dados
	 */
	public Record getRecord();
	
	/*
	 * Faz a chamada de escrita do record na posição em que estava
	 * Caso seja necessário, o objeto ira fazer chamadas de atualização da posição dos outros records
	 */
	public void update(Record r);

	
	/*
	 * Reinicia a leitura da stream, voltando para a primieira posição
	 */
	public void reset();


}
