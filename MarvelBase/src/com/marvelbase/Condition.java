package com.marvelbase;

import com.marvelbase.DataType.DataType;

public class Condition {

	public Column column;
	public String operation;
	public DataType value;

	public DataType getValue() {
		return this.value;
	}

	public Condition(Column column, String operation, DataType value) {
		this.column = column;
		this.operation = operation;
		this.value = value;
	}
	
	public boolean result(DataType tableValue) {
		switch (this.operation) {
			case "=":
				return tableValue.equal(this.value.getValue());
			case "<":
				return tableValue.lesser(this.value.getValue());
			case "<=":
				return tableValue.lesserEquals(this.value.getValue());
			case ">":
				return tableValue.greater(this.value.getValue());
			case ">=":
				return tableValue.greaterEquals(this.value.getValue());
			case "like":
				return tableValue.like(this.value.getValue());
			case "!=":
				return tableValue.notEqual(this.value.getValue());
			case "LIKE":
				return tableValue.like(this.value.getValue());
			default:
				return true;
		}
	}
}