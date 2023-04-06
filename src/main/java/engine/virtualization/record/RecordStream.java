package engine.virtualization.record;


import java.math.BigInteger;

public interface RecordStream {
	
	/*
	 * Abre e fecha o leitor sequencial
	 * É importante caso seja necessário bloquear a tabela dependendo do record manager
	 * Na função open deve ser passado true caso possa ser chamado a função write
	 */
	public void open(boolean lockToWrite);
	public void close();
	
	/*
	 * Verifica se existe um record para a próxima leitura
	 */
	public boolean hasNext() ;

	/*
	 * Retorna o record no ponteiro atual, e atualiza o ponteiro para o próximo ponteiro;
	 */
	public Record next();
	
	/*
	 * Faz a leitura do record ou a posição em que ele está armazendo no banco de dados
	 */
	public Record getRecord();
	
	/*
	 * Faz a chamada de escrita do record na posição em que estava
	 * Caso seja necessário, o objeto ira fazer chamadas de atualização da posição dos outros records
	 */
	public void write(Record r);

	/*
	 * Diz que o item atual deve ser definido como removido
	 */
	public void remove();
	
	/*
	 * Reinicia a leitura da stream, voltando para a primieira posição
	 */
	public void reset();
	/*
	 * Define a posição para a leitura, caso seja passado uma posição inválida, a leitura pode ocorrer de forma errada.
	 */
	public void setPointer(BigInteger pk);
	public BigInteger getPointer();
	
}
