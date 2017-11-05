package com.marvelbase.Commands;

import java.util.ArrayList;
import java.util.List;

class DeleteResult {

	private int numOfRecordsDeleted;
	private List<Integer> deletedKeysList;
	private boolean wholePageDeleted = false;
	private int rightSiblingPageNumber = -1;
	private boolean updateRightMostChildRightPointer = false;
	private int onePageNumber = -1;
	private boolean isLeaf = false;

	DeleteResult() {
		this.numOfRecordsDeleted = 0;
		this.deletedKeysList = new ArrayList<>();
	}
	
	public void setOnePageNumber(int onePageNumber) {
		this.onePageNumber = onePageNumber;
	}

	void deleteKey(int key) {
		this.numOfRecordsDeleted++;
		this.deletedKeysList.add(key);
	}
	
	public int getOnePageNumber() {
		return onePageNumber;
	}

	int getNumOfRecordsDeleted() {
		return this.numOfRecordsDeleted;
	}

	boolean keyIsDeleted(int key) {
		return this.deletedKeysList.contains(key);
	}
	
	void setLeaf(boolean leaf) {
		isLeaf = leaf;
	}

	boolean isWholePageDeleted() {
		return this.wholePageDeleted;
	}
	
	void setUpdateRightMostChildRightPointer(boolean updateRightMostChildRightPointer) {
		this.updateRightMostChildRightPointer = updateRightMostChildRightPointer;
	}

	int getRightSiblingPageNumber() {
		return this.rightSiblingPageNumber;
	}


	void setWholePageDeleted(boolean wholePageDeleted) {
		this.wholePageDeleted = wholePageDeleted;
	}

	void setRightSiblingPageNumber(int rightSiblingPageNumber) {
		this.rightSiblingPageNumber = rightSiblingPageNumber;
	}

	boolean isUpdateRightMostChildRightPointer() {
		return updateRightMostChildRightPointer;
	}

	

	public boolean isLeaf() {
		return isLeaf;
	}

	

	void mergeSubResult(DeleteResult subDeleteResult) {
		this.numOfRecordsDeleted += subDeleteResult.numOfRecordsDeleted;
		this.deletedKeysList.addAll(subDeleteResult.deletedKeysList);
	}

	

	
}
