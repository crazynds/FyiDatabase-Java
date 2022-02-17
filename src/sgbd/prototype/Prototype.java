package sgbd.prototype;

import java.util.ArrayList;
import java.util.Comparator;
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
		if(c == null)
			throw new DataBaseException("Prototype->addColumn","Column passada é nula");
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
	
	public TranslatorApi validateColumns()  {
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
				String error="Coluna "+x+" tem nome de tamanhos inválido!";
				ex=new DataBaseException("Prototype->ValidateColumns",error);
				ex.addValidation("Max:240");
				ex.addValidation("Min:1");
				throw ex;
			}
			if(col.isPrimaryKey() && col.camBeNull()){
				String error="Coluna "+col.getName()+" não pode ser nula e ao mesmo tempo ser primary key!";
				ex=new DataBaseException("Prototype->ValidateColumns",error);
				ex.addValidation("NULL not in PRIMARY KEY");
				throw ex;
			}
			if(col.isPrimaryKey() && col.isDinamicSize()){
				String error="Coluna "+col.getName()+" não pode ser dinamica ao mesmo tempo que é primary key!";
				ex=new DataBaseException("Prototype->ValidateColumns",error);
				ex.addValidation("DINAMIC not in PRIMARY KEY");
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

		columns.sort(new Comparator<Column>() {
			@Override
			public int compare(Column o1, Column o2) {
				if(o1.isPrimaryKey()&& !o2.isPrimaryKey())return -1;
				if(!o1.isPrimaryKey()&& o2.isPrimaryKey())return 1;
				if(o1.isPrimaryKey()&& o2.isPrimaryKey())
					return Integer.compare(columns.indexOf(o1),columns.indexOf(o2));

				else {
					if(o1.isDinamicSize() && !o2.isDinamicSize())return 1;
					if(!o1.isDinamicSize() && o2.isDinamicSize())return -1;
					if(o1.getSize()==o2.getSize())
						return o1.getName().compareTo(o2.getName());
					else
						return Integer.compare(o1.getSize(),o2.getSize());
				}
			}
		});
		return new TranslatorApi(columns);
	}

	@Override
	public Iterator<Column> iterator() {
		return columns.iterator();
	}

}
