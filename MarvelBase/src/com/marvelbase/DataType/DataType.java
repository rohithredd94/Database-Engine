package com.marvelbase.DataType;

public interface DataType<ADT> {

	ADT getValue();
	boolean equal(Object rightValue);
	boolean notEqual(Object rightValue);
	boolean greater(Object rightValue);
	boolean greaterEquals(Object rightValue);
	boolean lesser(Object rightValue);
	boolean lesserEquals(Object rightValue);
	boolean like(Object rightValue);
	byte[] getByteValue();
	byte getDataTypeOfValue();

}
