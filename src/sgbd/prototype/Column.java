package sgbd.prototype;

import engine.exceptions.DataBaseException;

public class Column {

	public static final byte NONE= 0;
	public static final byte DINAMIC_COLUMN_SIZE = (1<<0);
	public static final byte CAM_NULL_COLUMN = (1<<1);
	public static final byte SHIFT_8_SIZE_COLUMN = (1<<2);
	public static final byte SIGNED_INTEGER_COLUMN = (1<<3);
	public static final byte PRIMARY_KEY = (1<<4);
	
	protected short size;
	protected String name;
	protected byte flags;

	public Column(String name,short size,byte flags)  {
		this.size=size;
		this.name=name;
		this.flags=flags;
		checkErrors();
	}
	public Column(Column c)  {
		this.size=c.size;
		this.name=c.name;
		this.flags=c.flags;
		checkErrors();
	}
	
	private void checkErrors()  {
		if(size==0){
			String error="Uma coluna não pode ter tamanho zero!";
			String validator="size_column > 0";
			throw new DataBaseException("Column->Constructor",error,validator);
		}
		if(isShift8Size() && !isDinamicSize()){
			String error="Uma coluna com tamanho expandido deve ser dinamica!";
			String validator="SHIFT_8_SIZE + DINAMIC_SIZE == VALID";
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
	
	public int getSize() {
		if(isShift8Size())
			return size<<8;
		else
			return size;
	}

	public byte getFlags(){
		return flags;
	}
	
	public boolean isDinamicSize(){
		return (flags&DINAMIC_COLUMN_SIZE)!=0;
	}
	public boolean isShift8Size(){
		return (flags&SHIFT_8_SIZE_COLUMN)!=0;
	}
	public boolean isSignedInteger(){
		return (flags&SIGNED_INTEGER_COLUMN)!=0;
	}
	public boolean camBeNull(){
		return (flags&CAM_NULL_COLUMN)!=0;
	}
	public boolean isPrimaryKey(){
		return (flags&PRIMARY_KEY)!=0;
	}

	public String getName() {
		return name;
	}
	
}
