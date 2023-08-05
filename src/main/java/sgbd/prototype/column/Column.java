package sgbd.prototype.column;

import engine.exceptions.DataBaseException;
import sgbd.prototype.metadata.Metadata;

public class Column extends Metadata {

	protected String name;

	public Column(String name,short size,short flags)  {
		super(size,flags);
		this.name=name;
		checkErrors();
	}
	public Column(Column c)  {
		super(c);
		this.name=c.name;
		checkErrors();
	}
	
	private void checkErrors()  {
		if(getSize()==0){
			String error="Uma coluna não pode ter tamanho zero!";
			String validator="size_column > 0";
			throw new DataBaseException("Column->Constructor",error,validator);
		}
		if(isShift8Size() && !isDinamicSize()){
			String error="Uma coluna com tamanho expandido deve ser dinamica!";
			String validator="SHIFT_8_SIZE + DINAMIC_SIZE == VALID";
			throw new DataBaseException("Column->Constructor",error,validator);
		}
		if(isBoolean() && isDinamicSize()){
			String error="Uma coluna do tipo boolean não pode ter tamanho dinamico!";
			String validator="BOOLEAN + DINAMIC_SIZE == INVALID";
			throw new DataBaseException("Column->Constructor",error,validator);
		}
		int strl = name.length();
		if(strl>240 || strl==0){
			String error="Uma coluna com nome de tamanho inválido!";
			DataBaseException ex=new DataBaseException("Column->Constructor",error);
			ex.addValidation("Max:240");
			ex.addValidation("Min:1");
			throw ex;
		}
	}

	public String getName() {
		return name;
	}
	
}
