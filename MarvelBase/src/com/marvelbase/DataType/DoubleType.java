package com.marvelbase.DataType;


import java.nio.ByteBuffer;

public class DoubleType implements DataType<Double> {

	private Double value;

	public DoubleType(Double value) {
		this.value = value;
	}

	@Override
	public Double getValue() {
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
		return rightValue instanceof Double && this.value > (Double) rightValue;
	}

	@Override
	public boolean greaterEquals(Object rightValue) {
		return rightValue instanceof Double && this.value >= (Double) rightValue;
	}

	@Override
	public boolean lesser(Object rightValue) {
		return rightValue instanceof Double && this.value < (Double) rightValue;
	}

	@Override
	public boolean lesserEquals(Object rightValue) {
		return rightValue instanceof Double && this.value <= (Double) rightValue;
	}

	@Override
	public boolean like(Object rightValue) {
		return false;
	}

	@Override
	public byte[] getByteValue() {
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putDouble(this.value);
		return bb.array();
	}

	@Override
	public byte getDataTypeOfValue() {
		if(value == null)
			return 0x03;
		else
			return 0x09;
	}

}
