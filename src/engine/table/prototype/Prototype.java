package engine.table.prototype;

import java.util.ArrayList;
import java.util.Iterator;

import engine.exceptions.DataBaseException;

public class Prototype implements Iterable<Column>{
	
	public static final Long KB = (long) 1024;
	public static final Long MB = 1024*KB;
	public static final Long GB = 1024*MB;
	public static final Long TB = 1024*GB;
	
	private ArrayList<Column> columns;
	private boolean stat;
	
	private int headerSize = 0;
	
	public Prototype() {
		columns = new ArrayList<Column>();
		stat =true;
	}

	public void addColumn(Column c) {
		columns.add(c);
		if(c.isDinamicSize())stat=false;
	}
	public void addColumn(String name,short size,byte flags)  {
		Column c= new Column(name, size, flags);
		columns.add(c);
		if(c.isDinamicSize())stat=false;
	}

	public void addColumn(String name, int size, int flag)  {
		addColumn(name, (short)size,(byte)flag);
	}
	
	public boolean isStatic() {
		return stat;
	}
	
	public Column getColumn(int val) {
		return columns.get(val);
	}
	
	public short size() {
		return (short)columns.size();
	}
	
	public int getRecomendedChunckStorage()  {
		int sum=0;
		boolean isStatic=true;
		int qtd=1;


		int header=3; // Present, sobre size (3 bytes)
		for(Column a:columns){
			header++; // Column null bit
			sum+=a.getSize();
			if(a.isDinamicSize())isStatic=false;
		}
		if(sum==0){
			String error="Tamanho de cada linha igual tem que ser maior que zero.";
			DataBaseException e = new DataBaseException("PrototypeColumns->getRecomendedChunckStorage",error);
			e.addValidation("size(row) > 0B");
			e.addValidation("size(row) < 24MB");
			throw e;
		}else if(sum>24*MB){ // 3 bytes de tamanho 
			String error="Tamanho de cada linha deve ser de não maior que 24 MB.";
			DataBaseException e= new DataBaseException("PrototypeColumns->getRecomendedChunckStorage",error);
			e.addValidation("size(row) > 0B");
			e.addValidation("size(row) < 24MB");
			throw e;
		}
		headerSize=(int) (Math.ceil(header/8f));
		sum+=headerSize +((sum>=64*KB)?4:((sum>=256)?2:1));
		while(sum%512!=0 && isStatic){ // Max mult = *512 items
			qtd<<=1;
			sum<<=1;
		}
		while(sum%32!=0 && !isStatic){ // Max multi = *32 items
			qtd<<=1;
			sum<<=1;
		}
		if(isStatic){
			int aux=sum;
			int auxQtd=qtd;
			while(qtd<64|| sum < 16*KB){ //Min 512 items e 16 KB do tamanho de cluster
				qtd+=auxQtd;
				sum+=aux;
			}
		}else{
			int aux=sum;
			int auxQtd=qtd;
			while(qtd<8 || sum < 16*KB){ //Min 8 items e 16 Kb de tamanho de cluster
				qtd+=auxQtd;
				sum+=aux;
			}
		}
		this.stat=isStatic;
		return sum;
	}
	
	public void validateColumns()  {
		DataBaseException ex =null;
		if(size()==0){
			String error="Não é valido uma tabela com nenhuma coluna!";
			ex=new DataBaseException("Prototype->ValidateColumns",error);
			ex.addValidation("Min:1");
			throw ex;
		}
		for(short x=0;x<size();x++){
			Column col=getColumn(x);
			int namelen=col.getName().length();
			if(namelen>240 || namelen<1){
				String error="Coluna "+x+" tem nomes de tamanhos inválido!";
				ex=new DataBaseException("Prototype->ValidateColumns",error);
				ex.addValidation("Max:240");
				ex.addValidation("Min:1");
				throw ex;
			}
			for(short y=(short) (x+1);y<size();y++){
				Column col2=getColumn(y);
				if(col.getName().equalsIgnoreCase(col2.getName())){
					String error="Coluna "+x+" e a coluna "+y+" tem nomes iguais!";
					ex=new DataBaseException("Prototype->ValidateColumns",error);
					throw ex;
				}
			}
		}
	}

	@Override
	public Iterator<Column> iterator() {
		return columns.iterator();
	}

}
