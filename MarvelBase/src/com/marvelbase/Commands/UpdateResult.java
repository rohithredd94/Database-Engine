package com.marvelbase.Commands;

import java.util.ArrayList;
import java.util.List;

class UpdateResult {

	private List<Integer> newPksInserted;
	private List<Integer> deletedKeysList;
	private boolean wholePageDeleted;
	private boolean isLeaf;
	private boolean integrityViolated;
	private boolean conditionFailed;
	private int lastUpdatedPk;

	UpdateResult() {
		this.newPksInserted = new ArrayList<>();
		this.deletedKeysList = new ArrayList<>();
		this.integrityViolated = false;
		this.wholePageDeleted = false;
		this.isLeaf = false;
		this.conditionFailed = false;
		this.lastUpdatedPk = -1;
	}

	void keyChange(int key) {
		this.deletedKeysList.add(key);
	}

	void keyInserted(int key) {
		this.newPksInserted.add(key);
	}

	public List<Integer> getDeletedKeysList() {
		return deletedKeysList;
	}

	public List<Integer> getNewPksInserted() {
		return newPksInserted;
	}

	public void setNewPksInserted(List<Integer> newPksInserted) {
		this.newPksInserted = newPksInserted;
	}

	public void addNewPksInserted(List<Integer> newPksInserted) {
		this.newPksInserted.addAll(newPksInserted);
	}

	public boolean isLeaf() {
		return isLeaf;
	}

	public void setLeaf(boolean leaf) {
		isLeaf = leaf;
	}

	public boolean isWholePageDeleted() {
		return wholePageDeleted;
	}

	public void setWholePageDeleted(boolean wholePageDeleted) {
		this.wholePageDeleted = wholePageDeleted;
	}

	public boolean isIntegrityViolated() {
		return integrityViolated;
	}

	public void setIntegrityViolated(boolean integrityViolated) {
		this.integrityViolated = integrityViolated;
	}

	public boolean isConditionFailed() {
		return conditionFailed;
	}

	public void setConditionFailed(boolean conditionFailed) {
		this.conditionFailed = conditionFailed;
	}

	public int getLastUpdatedPk() {
		return lastUpdatedPk;
	}

	public void setLastUpdatedPk(int lastUpdatedPk) {
		this.lastUpdatedPk = lastUpdatedPk;
	}
}