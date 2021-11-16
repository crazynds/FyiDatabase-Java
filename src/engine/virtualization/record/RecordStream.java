package engine.virtualization.record;


public interface RecordStream {
	
	/*
	 * Abre e fecha o leitor sequencial
	 * É importante caso seja necessário bloquear a tabela dependendo do record manager
	 */
	public void open();
	public void close();
	
	/*
	 * Verifica se existe um record para a próxima leitura
	 */
	public boolean hasNext() ;

	/*
	 * Retorna o record no ponteiro atual, e atualiza o ponteiro para o próximo ponteiro;
	 */
	public Record next();
	public void next(byte[] buffer);
	
	/*
	 * Faz a leitura do record ou a posição em que ele está armazendo no banco de dados
	 */
	public Record getRecord();
	public void getRecord(byte[] buffer);
	
	/*
	 * Faz a chamada de escrita do record na posição em que estava
	 * Retorna a nova posição no arquivo. Pode ser a mesma em que estava
	 * Caso seja necessário, o objeto ira fazer chamadas de atualização da posição dos outros records
	 */
	public long write(Record r);
	
	/*
	 * Informa se os records estão ordenados 
	 */
	public boolean isOrdened();
	
	/*
	 * Reinicia a leitura da stream, voltando para a primieira posição
	 */
	public void reset();
	/*
	 * Define a posição para a leitura, caso seja passado uma posição inválida, a leitura pode ocorrer de forma errada.
	 */
	public void setPointer(long position);
	public long getPointer();
	
}
