package com.marvelbase;

import com.marvelbase.DataType.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class Column {

	private String name;
	private String type;
	private boolean isNullable = false;
	private int ordinalPosition;
	private String tableName;
	private String dbName;
	private boolean isPk = false;

	public Column(String name, String type, boolean isNullable, int ordinalPosition, String tableName, String dbName, boolean isPk) {
		this.dbName = dbName;
		this.name = name;
		this.type = type;
		this.isNullable = isNullable;
		this.ordinalPosition = ordinalPosition;
		this.tableName = tableName;
		this.isPk = isPk;
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	public boolean isNullable() {
		return isNullable;
	}

	public int getOrdinalPosition() {
		return ordinalPosition;
	}

	public String getTableName() {
		return tableName;
	}

	public String getDbName() {
		return dbName;
	}

	public boolean isPk() {
		return isPk;
	}

	public void writeValue(RandomAccessFile file, String value) throws IOException, ParseException {
		switch (this.type.toUpperCase()) {
			case "INT":
				if(value == null) {
					file.writeByte(0x02);
					file.writeInt(0);
				} else {
					file.writeByte(0x06);
					file.writeInt(Integer.parseInt(value));
				}
				break;
			case "TINYINT":
				if(value == null) {
					file.writeByte(0x00);
					file.writeByte(0x00);
				} else {
					file.writeByte(0x06);
					file.writeByte(Byte.parseByte(value));
				}
				break;
			case "SMALLINT":
				if(value == null) {
					file.writeByte(0x01);
					file.writeShort(0);
				} else {
					file.writeByte(0x05);
					file.writeShort(Short.parseShort(value));
				}
				break;
			case "BIGINT":
				if(value == null) {
					file.writeByte(0x03);
					file.writeLong(0);
				} else {
					file.writeByte(0x07);
					file.writeLong(Long.parseLong(value));
				}
				break;
			case "REAL":
				if(value == null) {
					file.writeByte(0x02);
					file.writeFloat(0);
				} else {
					file.writeByte(0x08);
					file.writeFloat(Float.parseFloat(value));
				}
				break;
			case "DOUBLE":
				if(value == null) {
					file.writeByte(0x03);
					file.writeDouble(0);
				} else {
					file.writeByte(0x09);
					file.writeDouble(Double.parseDouble(value));
				}
				break;
			case "DATETIME":
				if(value == null) {
					file.writeByte(0x03);
					file.writeDouble(0);
				} else {
					file.writeByte(0x0A);
					SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
					Date value_date = parser.parse(value);
					Calendar calendar = Calendar.getInstance();
					calendar.setTime(value_date);
					long epochSeconds = calendar.getTimeInMillis() / 1000;
					file.writeLong(epochSeconds);
				}
				break;
			case "DATE":
				if(value == null) {
					file.writeByte(0x03);
					file.writeDouble(0);
				} else {
					file.writeByte(0x0B);
					SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd");
					Date value_date = parser.parse(value);
					Calendar calendar = Calendar.getInstance();
					calendar.setTime(value_date);
					long epochSeconds = calendar.getTimeInMillis() / 1000;
					System.out.println(epochSeconds);
					file.writeLong(epochSeconds);
				}
				break;
			case "TEXT":
				if(value == null) {
					file.write(0x0C);
				} else {
					file.write(0x0C + value.length());
					file.write(value.getBytes(StandardCharsets.US_ASCII));
				}
				break;
		}
	}

	public boolean isDataTypeCorrect(byte value) {
		switch (this.type.toUpperCase()) {
			case "INT":
				return value == 0x02 || value == 0x06;
			case "TINYINT":
				return value == 0x00 || value == 0x04;
			case "SMALLINT":
				return value == 0x01 || value == 0x05;
			case "BIGINT":
				return value == 0x03 || value == 0x07;
			case "REAL":
				return value == 0x02 || value == 0x08;
			case "DOUBLE":
				return value == 0x03 || value == 0x09;
			case "DATETIME":
				return value == 0x03 || value == 0x0A;
			case "DATE":
				return value == 0x03 || value == 0x0B;
			case "TEXT":
				return value >= 0x0C;
			default:
				return false;
		}
	}
	
	public boolean check(String value) {
		if(value == null && !isNullable && !this.type.equals("TEXT"))
			return false;
		switch (this.type.toUpperCase()) {
			case "INT":
				try {
					if (value != null)
						Integer.parseInt(value);
				} catch (NumberFormatException e) {
					return false;
				}
				break;
			case "TINYINT":
				try {
					if (value != null)
						Byte.parseByte(value);
				} catch (NumberFormatException e) {
					return false;
				}
				break;
			case "SMALLINT":
				try {
					if (value != null)
						Short.parseShort(value);
				} catch (NumberFormatException e) {
					return false;
				}
				break;
			case "BIGINT":
				try {
					if (value != null)
						Long.parseLong(value);
				} catch (NumberFormatException e) {
					return false;
				}
				break;
			case "REAL":
				try {
					if (value != null)
						Float.parseFloat(value);
				} catch (NumberFormatException e) {
					return false;
				}
				break;
			case "DOUBLE":
				try {
					if (value != null)
						Double.parseDouble(value);
				} catch (NumberFormatException e) {
					return false;
				}
				break;
			case "DATETIME":
				SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
				try {
					parser.parse(value);
				} catch (ParseException e) {
					return false;
				}
				break;
			case "DATE":
				parser = new SimpleDateFormat("yyyy-MM-dd");
				try {
					parser.parse(value);
				} catch (ParseException e) {
					return false;
				}
				break;
			case "TEXT":
				return true;
			default:
				return false;
		}
		return true;
	}

	

	public String getRecordValue(byte[] bytes) {
		ByteBuffer bb = ByteBuffer.allocate(bytes.length);
		bb.put(bytes);
		switch (this.type.toUpperCase()) {
			case "INT":
				return bb.getInt(0) + "";
			case "TINYINT":
				return bb.get(0) + "";
			case "SMALLINT":
				return bb.getShort(0) + "";
			case "BIGINT":
				return bb.getLong(0) + "";
			case "REAL":
				return bb.getFloat(0) + "";
			case "DOUBLE":
				return bb.getDouble(0) + "";
			case "DATETIME":
				SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
				Calendar calendar = Calendar.getInstance();
				calendar.setTimeInMillis(bb.getLong() * 1000);
				return parser.format(calendar.getTime());
			case "DATE":
				parser = new SimpleDateFormat("yyyy-MM-dd");
				calendar = Calendar.getInstance();
				calendar.setTimeInMillis(bb.getLong() * 1000);
				return parser.format(calendar.getTime());
			case "TEXT":
				return new String(bytes, StandardCharsets.US_ASCII);
			default:
				return null;
		}
	}
	
	public short getTypeLength(String value) {
		switch (this.type.toUpperCase()) {
			case "INT":
				return 4;
			case "TINYINT":
				return 1;
			case "SMALLINT":
				return 2;
			case "BIGINT":
				return 8;
			case "REAL":
				return 4;
			case "DOUBLE":
				return 8;
			case "DATETIME":
				return 8;
			case "DATE":
				return 8;
			case "TEXT":
				if(value != null)
					return (short) value.length();
				else
					return 0;
			default:
				return -1;
		}
	}
	
	public String getFormat() {
		switch (type.toUpperCase()) {
			case "INT":
				return "%-8s";
			case "TINYINT":
				return "%-3s";
			case "SMALLINT":
				return "%-5s";
			case "BIGINT":
				return "%-12s";
			case "REAL":
				return "%-8s";
			case "DOUBLE":
				return "%-12s";
			case "DATETIME":
				return "%-19s";
			case "DATE":
				return "%-10s";
			case "TEXT":
				return "%-30s";
			default:
				return null;
		}
	}

	public DataType getColumnValue(String value) throws ParseException {
		switch (this.type.toUpperCase()) {
			case "INT":
				return new IntType(Integer.parseInt(value));
			case "TINYINT":
				return new TinyInt(Byte.parseByte(value));
			case "SMALLINT":
				return new SmallInt(Short.parseShort(value));
			case "BIGINT":
				return new BigInt(Long.parseLong(value));
			case "REAL":
				return new Real(Float.parseFloat(value));
			case "DOUBLE":
				return new DoubleType(Double.parseDouble(value));
			case "DATETIME":
				SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
				Date value_date = parser.parse(value);
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(value_date);
				long epochSeconds = calendar.getTimeInMillis() / 1000;
				return new DateTimeType(epochSeconds);
			case "DATE":
				parser = new SimpleDateFormat("yyyy-MM-dd");
				value_date = parser.parse(value);
				calendar = Calendar.getInstance();
				calendar.setTime(value_date);
				epochSeconds = calendar.getTimeInMillis() / 1000;
				return new DateType(epochSeconds);
			case "TEXT":
				return new TextType(value);
			default:
				return null;
		}
	}
	
	@Override
	public String toString() {
		return "Column{" +
				"name='" + name + '\'' +
				", type='" + type + '\'' +
				", isNullable=" + isNullable +
				", ordinalPosition=" + ordinalPosition +
				", tableName='" + tableName + '\'' +
				", isPk=" + isPk +
				'}';
	}

	public boolean checkCreation() {
		List<String> dataType = Arrays.asList("INT", "TINYINT", "SMALLINT", "BIGINT", "REAL", "DOUBLE", "DATETIME", "DATE", "TEXT");
		return dataType.contains(this.type.toUpperCase());
	}
}