package engine.table.index;

import java.util.Iterator;


public class TreeNode<T> implements Iterable<TreeNode<T>> {
	Long key;
	T data;
	TreeNode<T> parent;
	
	TreeNode<T> maior;
	TreeNode<T> menor;	
	public TreeNode(int key,T data) {
	    this.key = new Long(key);
	    this.data = data;
	    maior=null;
	    menor=null;
	}
	
	public TreeNode(Long key,T data) {
	    this.key = key;
	    this.data = data;
	    maior=null;
	    menor=null;
	}
	
	public void addChild(Long key,T filho) {
	    if(this.key<key) {
	    	addChildMajor(key,filho);
	    }else {
	    	addChildMinor(key,filho);
	    }
	}
	
	public void print(String path) {
		System.out.println(path+""+this.getKey()+" => "+this.data);
		if(menor==null&&maior==null)return;
		if(menor!=null) {
			menor.print(path+"-");
		}
		System.out.println(path+"");
		if(maior!=null) {
			maior.print(path+"-");
		}
	}
	public T getChild(Long key) {
		if(this.key.equals(key))return data;
		else if(this.key<key) {
			if(maior==null)return null;
			else return maior.getChild(key);
		}else {
			if(menor==null)return null;
			else return menor.getChild(key);
		}
	}
	
	private void addChildMajor(Long key,T filho) {
		if(maior!=null) {
			if(menor==null) {
				setMenor(this.key,this.data);
				if(maior.getKey()>key) {
					setActual(key, filho);
				}else {
					setActual(maior.key, maior.data);
					setMaior(key, filho);
				}
			}else {
				if(isPerfect()) {
					TreeNode<T> aux=new TreeNode<T>(key,filho);
					troca(aux);
					this.menor=aux;
				}else {
					maior.addChild(key, filho);
				}
			}
		}else {
			setMaior(key, filho);
		}
	}

	private void addChildMinor(Long key,T filho) {
		if(menor!=null) {
			if(maior==null) {
				setMaior(this.key,this.data);
				if(menor.getKey()<key) {
					setActual(key, filho);
				}else {
					setActual(maior.key, maior.data);
					setMenor(key, filho);
				}
			}else {
				if(isPerfect()) {
					TreeNode<T> aux=new TreeNode<T>(key,filho);
					troca(aux);
					this.maior=aux;
				}else {
					menor.addChild(key, filho);
				}
			}
		}else {
			setMenor(key, filho);
		}
	}
	
	private void troca(TreeNode<T> t) {
		Long aux=this.key;
		T data=this.data;
		TreeNode<T> taux = this.menor;
		this.key=t.key;
		this.data=t.data;
		this.menor=t.menor;
		t.key=aux;
		t.data=data;
		t.menor=taux;
		taux=this.maior;
		this.maior=t.maior;
		t.maior=taux;
	}
	private void setActual(Long key,T filho) {
		this.data=maior.data;
		this.key=maior.key;
	}
	private void setMenor(Long key,T filho) {
		menor=new TreeNode<T>(key,filho);
	}
	private void setMaior(Long key,T filho) {
		maior=new TreeNode<T>(key,filho);
	}
	public boolean isPerfect() {
		if(menor==null||maior==null)return false;
		if(key-menor.getKey()==1 && maior.getKey()-key==1)return true;
		if(!menor.isPerfect())return false;
		if(!maior.isPerfect())return false;
		if(key-menor.getKey()==maior.getKey()-key)return true;
		return false;
	}
	
	public Long getKey() {
		return key;
	}

	@Override
	public Iterator<TreeNode<T>> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

}