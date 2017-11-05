package com.marvelbase.Commands;

import com.marvelbase.Table;

import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.LinkedHashMap;

public class InsertParams {

	private RandomAccessFile tableFile;
	private int primaryKey;
	private int pageNumber;
	private Table table;
	private byte[] record;
	private LinkedHashMap<String, String> values;

	public InsertParams(RandomAccessFile tableFile, int primaryKey, int pageNumber, LinkedHashMap<String, String> values, Table table, byte[] record) {
		this.tableFile = tableFile;
		this.primaryKey = primaryKey;
		this.pageNumber = pageNumber;
		this.values = values;
		this.table = table;
		this.record = record;
	}
	
	public Table getTable() {
		return table;
	}

	public RandomAccessFile getTableFile() {
		return tableFile;
	}
	
	public int getPageNumber() {
		return pageNumber;
	}

	public int getPrimaryKey() {
		return primaryKey;
	}

	public LinkedHashMap<String, String> getValues() {
		return values;
	}

	

	public byte[] getRecord() {
		return record;
	}

	@Override
	public String toString() {
		return "InsertParams{" +
				"tableFile=" + tableFile +
				", primaryKey=" + primaryKey +
				", pageNumber=" + pageNumber +
				", table=" + table +
				", record=" + Arrays.toString(record) +
				", values=" + values +
				'}';
	}
}
