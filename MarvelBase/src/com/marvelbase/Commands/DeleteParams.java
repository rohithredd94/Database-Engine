package com.marvelbase.Commands;

import com.marvelbase.Condition;
import com.marvelbase.Table;

import java.io.RandomAccessFile;

public class DeleteParams {

	private RandomAccessFile file;
	private int pageNumber;
	private Table table;
	private Condition condition;

	public DeleteParams(RandomAccessFile file, int pageNumber, Table table, Condition condition) {
		this.file = file;
		this.pageNumber = pageNumber;
		this.table = table;
		this.condition = condition;
	}
	
	public Table getTable() {
		return table;
	}

	public RandomAccessFile getFile() {
		return file;
	}
	
	public Condition getCondition() {
		return condition;
	}

	public int getPageNumber() {
		return pageNumber;
	}	
}
