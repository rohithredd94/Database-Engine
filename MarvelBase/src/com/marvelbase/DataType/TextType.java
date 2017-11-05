package com.marvelbase.DataType;

import com.marvelbase.UtilityTools;

public class TextType implements DataType<String> {

	private String value;

	public TextType(String value) {
		this.value = value;
	}

	@Override
	public String getValue() {
		return this.value;
	}

	@Override
	public boolean equal(Object rightValue) {
		return rightValue instanceof String && this.value.equals(rightValue);
	}

	@Override
	public boolean notEqual(Object rightValue) {
		return rightValue instanceof String && !this.value.equals(rightValue);
	}

	@Override
	public boolean greater(Object rightValue) {
		return rightValue instanceof String && this.value.compareTo((String) rightValue) > 0;
	}

	@Override
	public boolean greaterEquals(Object rightValue) {
		return rightValue instanceof String && this.value.compareTo((String) rightValue) >= 0;
	}

	@Override
	public boolean lesser(Object rightValue) {
		return rightValue instanceof String && this.value.compareTo((String) rightValue) < 0;
	}

	@Override
	public boolean lesserEquals(Object rightValue) {
		return rightValue instanceof String && this.value.compareTo((String) rightValue) <= 0;
	}

	@Override
	public boolean like(Object rightValue) {
		//Replace all the Literals with their escapes which are not in like.
		//Replace all the ^ which are not escaped to
		if(!(rightValue instanceof String))
			return false;
		rightValue = UtilityTools.applyRegexSubstitution((String) rightValue, "(?<!%)((?:%%)*)(%\\^)", "$1\\\\^");
		//Replace all the [ which are not escaped to \[
		rightValue = UtilityTools.applyRegexSubstitution((String) rightValue, "(?<!%)((?:%%)*)(%\\[)", "$1\\\\[");
		//Escape all regex escaped characters.
		rightValue = UtilityTools.applyRegexSubstitution((String) rightValue, "([$.|?*+()\\{}])", "\\\\$1");
		//Replace all the _ which are not escaped to .
		rightValue = UtilityTools.applyRegexSubstitution((String) rightValue, "(?<!%)((?:%%)*)(_)", "$1.");
		//Replace all the % which are not escaped to .*
		rightValue = UtilityTools.applyRegexSubstitution((String) rightValue, "(?<!%)((?:%%)*)(%)(?=[^%]|$)", "$1.*");
		//Replace all %% to %
		rightValue = UtilityTools.applyRegexSubstitution((String) rightValue, "%%", "%");
		//Return if the value satisfies regex.
		return UtilityTools.regexSatisfy(this.value, "^" + rightValue + "$");
	}

	@Override
	public byte[] getByteValue() {
		return value.getBytes();
	}

	@Override
	public byte getDataTypeOfValue() {
		if(value == null)
			return 0x0C;
		else
			return (byte) (0x0C + value.length());
	}

}
