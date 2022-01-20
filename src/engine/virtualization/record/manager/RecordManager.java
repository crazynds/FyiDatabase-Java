package engine.virtualization.record.manager;

import java.math.BigInteger;
import java.util.List;

import engine.file.FileManager;
import engine.file.buffers.BlockBuffer;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInfoExtraction;
import engine.virtualization.record.RecordInterface;
import engine.virtualization.record.RecordStream;

public abstract class RecordManager{
	
	protected FileManager fileManager;
	protected RecordInfoExtraction recordInterface;
	
	public RecordManager(FileManager fm,RecordInfoExtraction ri) {
		this.fileManager=fm;
		this.recordInterface=ri;
	}

	protected BlockBuffer getBlockBuffer() {
		return fileManager.getBuffer();
	}
	protected RecordInfoExtraction getRecordInterface() {
		return recordInterface;
	}
	protected FileManager getFileManager() {
		return fileManager;
	}
	
	/*
	 * Inicia um arquivo do zero
	 * Reinicia todos os dados necessários
	 */
	public abstract void restart() ;
	
	/*
	 * Força os buffers a liberarem as modificações escritas
	 */
	public void flush(){
		fileManager.flush();
	}
	
	/*
	 * Fecha a manipulação do arquivo e faz o salvametno dos dados
	 */
	public void close(){
		fileManager.close();
	}

	/*
	 * Le um record a partir de uma chave primaria
	 */
	public abstract Record read(BigInteger pk);
	public abstract void read(BigInteger pk,byte[] buffer);
	
	/*
	 * Essa função tem como objetivo procurar algum record que tenha a chave primaria correspondente
	 * e atualiza-la com as informações do record correspondente.
	 * Caso não encontre ela deve ser adicionada a lista
	 */
	public abstract void write(Record r) ;
	public abstract void write(List<Record> list) ;
	
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
