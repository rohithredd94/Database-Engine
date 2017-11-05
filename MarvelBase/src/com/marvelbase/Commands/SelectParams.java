package com.marvelbase.Commands;

import com.marvelbase.Column;
import com.marvelbase.Condition;
import com.marvelbase.Table;

import java.io.RandomAccessFile;
import java.util.List;

public class SelectParams {

	private RandomAccessFile file;
	private int pageNumber;
	private List<Column> selectColumns;
	private Table table;
	private Condition condition;
	private boolean checkCondition;

	public SelectParams(RandomAccessFile file, int pageNumber, List<Column> selectColumns, Table table, Condition condition, boolean checkCondition) {
		this.file = file;
		this.pageNumber = pageNumber;
		this.selectColumns = selectColumns;
		this.table = table;
		this.condition = condition;
		this.checkCondition = checkCondition;
	}
	
	public Table getTable() {
		return table;
	}
	
	public Condition getCondition() {
		return condition;
	}

	public RandomAccessFile getFile() {
		return file;
	}
	
	public boolean isCheckCondition() {
		return checkCondition;
	}

	public int getPageNumber() {
		return pageNumber;
	}

	public List<Column> getSelectColumns() {
		return selectColumns;
	}

}
