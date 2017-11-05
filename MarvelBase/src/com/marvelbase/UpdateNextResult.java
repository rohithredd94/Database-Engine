package com.marvelbase;

public class UpdateNextResult {

	private int lastUpdatedPk;
	private int lastUpdatedIndex;
	private int newInsertedPk;
	private int oldDeletedPk;
	private boolean integrityViolated;
	private boolean conditionFailed;
	
	public int getLastUpdatedPk() {
		return lastUpdatedPk;
	}

	public int getLastUpdatedIndex() {
		return lastUpdatedIndex;
	}

	public int getNewInsertedPk() {
		return newInsertedPk;
	}

	public boolean isIntegrityViolated() {
		return integrityViolated;
	}

	public boolean isConditionFailed() {
		return conditionFailed;
	}

	public UpdateNextResult(int lastUpdatedPk, int lastUpdatedIndex) {
		this(lastUpdatedPk, lastUpdatedIndex, -1, -1);
	}

	public UpdateNextResult(int lastUpdatedPk, int lastUpdatedIndex, int newInsertedPk, int oldDeletedPk) {
		this(lastUpdatedPk, lastUpdatedIndex, newInsertedPk, oldDeletedPk, false, false);
	}

	public UpdateNextResult(int lastUpdatedPk, int lastUpdatedIndex, boolean integrityViolated) {
		this(lastUpdatedPk, lastUpdatedIndex, -1, -1, integrityViolated, false);
	}

	public UpdateNextResult(int lastUpdatedPk, int lastUpdatedIndex, boolean integrityViolated, boolean conditionFailed) {
		this(lastUpdatedPk, lastUpdatedIndex, -1, -1, integrityViolated, conditionFailed);
	}

	public UpdateNextResult(int lastUpdatedPk, int lastUpdatedIndex, int newInsertedPk, int oldDeletedPk, boolean integrityViolated, boolean conditionFailed) {
		this.lastUpdatedPk = lastUpdatedPk;
		this.lastUpdatedIndex = lastUpdatedIndex;
		this.newInsertedPk = newInsertedPk;
		this.oldDeletedPk = oldDeletedPk;
		this.integrityViolated = integrityViolated;
		this.conditionFailed = conditionFailed;
	}

	
}
