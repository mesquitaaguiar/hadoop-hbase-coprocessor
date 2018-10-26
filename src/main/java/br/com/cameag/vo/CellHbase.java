package br.com.cameag.vo;

import java.io.Serializable;
import java.util.Arrays;

import br.com.cameag.main.HBaseCoprocessor;

public class CellHbase implements Serializable{
	
	private static final long serialVersionUID = 5097266732369884176L;
	
	byte[] family;
	byte[] qualifier;
	byte[] value;
	
	public CellHbase(byte[] family, byte[] qualifier, byte[] value) {
		super();
		this.family = family;
		this.qualifier = qualifier;
		this.value = value;
	}

	public byte[] getFamily() {
		return family;
	}

	public byte[] getQualifier() {
		return qualifier;
	}

	public byte[] getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		return  (30 * 100 + + Arrays.hashCode(family)) +
						(30 * 100 + + Arrays.hashCode(qualifier));
	}

	@Override
	public boolean equals(Object obj) {
		
		if (this == obj)
			return true;
		else if (obj == null || getClass() != obj.getClass())
			return false;
		
		CellHbase other = (CellHbase) obj;
		if (!getOTHBaseCoprocessor().equals(other.getOTHBaseCoprocessor()))
			return false;
		else if (!Arrays.equals(family, other.family))
			return false;
		else if (!Arrays.equals(qualifier, other.qualifier))
			return false;
		
		return true;
	}

	private HBaseCoprocessor getOTHBaseCoprocessor() {
		return new HBaseCoprocessor();
	}
	
}
