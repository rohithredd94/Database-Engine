package com.marvelbase.DataType;

import java.nio.ByteBuffer;

public class Real implements DataType<Float> {

	private Float value;

	public Real(Float value) {
		this.value = value;
	}

	@Override
	public Float getValue() {
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
		return rightValue instanceof Float && this.value > (Float) rightValue;
	}

	@Override
	public boolean greaterEquals(Object rightValue) {
		return rightValue instanceof Float && this.value >= (Float) rightValue;
	}

	@Override
	public boolean lesser(Object rightValue) {
		return rightValue instanceof Float && this.value < (Float) rightValue;
	}

	@Override
	public boolean lesserEquals(Object rightValue) {
		return rightValue instanceof Float && this.value <= (Float) rightValue;
	}

	@Override
	public boolean like(Object rightValue) {
		return false;
	}

	@Override
	public byte[] getByteValue() {
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putFloat(this.value);
		return bb.array();
	}

	@Override
	public byte getDataTypeOfValue() {
		if(value == null)
			return 0x02;
		else
			return 0x08;
	}
}
