package com.marvelbase.DataType;

import java.nio.ByteBuffer;

public class IntType implements DataType<Integer> {

	private Integer value;

	public IntType(Integer value) {
		this.value = value;
	}

	@Override
	public Integer getValue() {
		return this.value;
	}

	@Override
	public boolean equal(Object rightValue) {
		return this.value.equals(rightValue);
	}

	@Override
	public boolean notEqual(Object rightValue) {
		return !this.value.equals(rightValue);
	}

	@Override
	public boolean greater(Object rightValue) {
		return rightValue instanceof Integer && this.value > (Integer) rightValue;
	}

	@Override
	public boolean greaterEquals(Object rightValue) {
		return rightValue instanceof Integer && this.value >= (Integer) rightValue;
	}

	@Override
	public boolean lesser(Object rightValue) {
		return rightValue instanceof Integer && this.value < (Integer) rightValue;
	}

	@Override
	public boolean lesserEquals(Object rightValue) {
		return rightValue instanceof Integer && this.value <= (Integer) rightValue;
	}

	@Override
	public boolean like(Object rightValue) {
		return false;
	}

	@Override
	public byte[] getByteValue() {
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(this.value);
		return bb.array();
	}

	@Override
	public byte getDataTypeOfValue() {
		if(value == null)
			return 0x02;
		else
			return 0x06;
	}

}
