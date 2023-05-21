package sgbd.prototype;

import engine.exceptions.DataBaseException;

public class Column {

	public static final short NONE= 0;
	public static final short DINAMIC_COLUMN_SIZE = (1<<0);
	public static final short CAN_NULL_COLUMN = (1<<1);
	public static final short LSHIFT_8_SIZE_COLUMN = (1<<2);
	public static final short SIGNED_INTEGER_COLUMN = (1<<3);
	public static final short PRIMARY_KEY = (1<<4);
	public static final short STRING = (1<<5);
	public static final short FLOATING_POINT = (1<<6);
	public static final short BOOLEAN = (1<<7);
	
	protected short size;
	protected short flags;
	protected String name;

	public Column(String name,short size,short flags)  {
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
	
	public int getSize() {
		if(isBoolean())return 1;
		if(isShift8Size())
			return size<<8;
		else
			return size;
	}

	public short getFlags(){
		return flags;
	}
	
	public boolean isDinamicSize(){
		return (flags&DINAMIC_COLUMN_SIZE)!=0;
	}
	public boolean isShift8Size(){
		return (flags&LSHIFT_8_SIZE_COLUMN)!=0;
	}
	public boolean isSignedInteger(){
		return (flags&SIGNED_INTEGER_COLUMN)!=0;
	}
	public boolean camBeNull(){
		return (flags&CAN_NULL_COLUMN)!=0;
	}
	public boolean isPrimaryKey(){
		return (flags&PRIMARY_KEY)!=0;
	}

	public String getName() {
		return name;
	}

	public boolean isInt(){
		return (!isString() && (flags&FLOATING_POINT)==0 && !isDinamicSize()) || (flags&SIGNED_INTEGER_COLUMN)!=0;
	}
	public boolean isDouble(){
		return (flags&FLOATING_POINT)!=0 && size==8;
	}
	public boolean isFloat(){
		return (flags&FLOATING_POINT)!=0 && size==4;
	}
	public boolean isString(){
		return (flags&STRING)!=0;
	}
	public boolean isBoolean(){
		return (flags&BOOLEAN)!=0;
	}
	
}
