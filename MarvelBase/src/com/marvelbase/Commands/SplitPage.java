package com.marvelbase.Commands;

public class SplitPage {

	private int key;
	private int pageNumber;
	private boolean inserted;
	private boolean shouldSplit = false;
	private int insertedSize = 0;

	public SplitPage(int key, int pageNumber, int insertedSize) {
		this.key = key;
		this.pageNumber = pageNumber;
		this.inserted = true;
		this.shouldSplit = true;
		this.insertedSize = insertedSize;
	}

	public SplitPage(int key) {
		this(key, false);
	}

	public SplitPage(int key, boolean inserted) {
		this.key = key;
		this.inserted = inserted;
	}

	public SplitPage(boolean inserted, int insertedSize) {
		this.inserted = inserted;
		this.shouldSplit = false;
		this.insertedSize = insertedSize;
	}
	
	public int getPageNumber() {
		return pageNumber;
	}

	public int getKey() {
		return key;
	}
	
	public int getInsertedSize() {
		return insertedSize;
	}

	public boolean isInserted() {
		return inserted;
	}

	public boolean isShouldSplit() {
		return shouldSplit;
	}

}
