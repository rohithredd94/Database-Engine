package com.marvelbase.DataType;

import java.nio.ByteBuffer;

public class SmallInt implements DataType<Short> {

	private Short value;

	public SmallInt(Short value) {
		this.value = value;
	}

	@Override
	public Short getValue() {
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
		return rightValue instanceof Short && this.value > (Short) rightValue;
	}

	@Override
	public boolean greaterEquals(Object rightValue) {
		return rightValue instanceof Short && this.value >= (Short)rightValue;
	}

	@Override
	public boolean lesser(Object rightValue) {
		return rightValue instanceof Short && this.value < (Short) rightValue;
	}

	@Override
	public boolean lesserEquals(Object rightValue) {
		return rightValue instanceof Short && this.value <= (Short) rightValue;
	}

	@Override
	public boolean like(Object rightValue) {
		return false;
	}

	@Override
	public byte[] getByteValue() {
		ByteBuffer bb = ByteBuffer.allocate(2);
		bb.putShort(this.value);
		return bb.array();
	}

	@Override
	public byte getDataTypeOfValue() {
		if(value == null)
			return 0x01;
		else
			return 0x05;
	}

}
