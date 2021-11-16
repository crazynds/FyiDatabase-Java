package engine.virtualization.record.manager;

import java.math.BigInteger;

import engine.file.FileManager;
import engine.file.blocks.BlockBuffer;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInterface;
import engine.virtualization.record.RecordStream;

public abstract class RecordManager{
	
	protected FileManager fileManager;
	protected RecordInterface recordInterface;
	
	public RecordManager(FileManager fm,RecordInterface ri) {
		this.fileManager=fm;
		this.recordInterface=ri;
	}

	protected BlockBuffer getBlockBuffer() {
		return fileManager.getBuffer();
	}
	protected RecordInterface getRecordInterface() {
		return recordInterface;
	}
	protected FileManager getFileManager() {
		return fileManager;
	}
	
	/*
	 * Inicia um arquivo do zero
	 * Reinicia todos os dados necessários
	 */
	public abstract void restart(FileManager fm) ;
	
	/*
	 * Abre um arquivo e carrega as informações necessárias
	 */
	public abstract void open(FileManager fm) ;
	
	/*
	 * Força os buffers a liberarem as modificações escritas
	 */
	public abstract void flush() ;
	
	/*
	 * Fecha a manipulação do arquivo e faz o salvametno dos dados
	 */
	public abstract void close() ;

	/*
	 * Le um record a partir de uma chave de armazenamento
	 * Essa chave é gerenciada pelo record manager e não é necessária ser salva, mas 
	 * torna a tarefa do record manager mais facil caso enviada
	 */
	public abstract Record read(BigInteger pk);
	public abstract void read(BigInteger pk,byte[] buffer);
	
	/*
	 * Essa função tem como objetivo procurar algum record que tenha a chave primaria correspondente
	 * e atualiza-la com as informações do record correspondente.
	 * Caso não encontre ela deve ser adicionada a lista
	 */
	public abstract Object write(Record r,BigInteger pk) ;
	
	/*
	 * Retorna true se o record manager garante os dados ordenados
	 */
	public abstract boolean isOrdened();	
	
	/*
	 * Retorna um objeto que vai fazer a leitura sequencial dos records.
	 * Esse objeto possui funcões auxiliares de controle
	 */
	public abstract RecordStream sequencialRead();
	
}
