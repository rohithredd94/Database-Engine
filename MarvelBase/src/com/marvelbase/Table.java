package com.marvelbase;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Table {

	private String tableName;
	private String dbName;
	private LinkedHashMap<String, Column> columns;
	private int recordCount;
	private int avgLength;
	private Column pkColumn = null;


	public Table(String dbName, String tableName, LinkedHashMap<String, Column> columns) {
		this.dbName = dbName;
		this.tableName = tableName;
		this.columns = columns;
	}

	public String getTableName() {
		return tableName;
	}

	public LinkedHashMap<String, Column> getColumns() {
		return columns;
	}

	public int getAvgLength() {
		return avgLength;
	}

	public void setAvgLength(int avgLength) {
		this.avgLength = avgLength;
	}

	public int getRecordCount() {
		return recordCount;
	}

	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}

	public String getDbName() {
		return dbName;
	}

	public Record getRecord(LinkedHashMap<String, String> values) throws ParseException {
		int recordLength = this.getRecordLength(values);
		ByteBuffer byteBuffer = ByteBuffer.allocate(recordLength);
		byteBuffer.putShort((short) (recordLength - 6));
		byteBuffer.putInt(10);
		byteBuffer.put((byte) columns.size());
		int pk = -1;
		for(Map.Entry<String, Column> entry : columns.entrySet()) {
			String key = entry.getKey();
			Column column = entry.getValue();
			String value = values.get(key);
			if(!column.isPk()) {
				switch (column.getType()) {
					case "INT":
						if(value == null) {
							byteBuffer.put((byte) 0x02);
							byteBuffer.putInt(0);
						} else {
							byteBuffer.put((byte) 0x06);
							byteBuffer.putInt(Integer.parseInt(value));
						}
						break;
					case "TINYINT":
						if(value == null) {
							byteBuffer.put((byte) 0x00);
							byteBuffer.put((byte) 0x00);
						} else {
							byteBuffer.put((byte) 0x06);
							byteBuffer.put(Byte.parseByte(value));
						}
						break;
					case "SMALLINT":
						if(value == null) {
							byteBuffer.put((byte) 0x01);
							byteBuffer.putShort((short) 0);
						} else {
							byteBuffer.put((byte) 0x05);
							byteBuffer.putShort(Short.parseShort(value));
						}
						break;
					case "BIGINT":
						if(value == null) {
							byteBuffer.put((byte) 0x03);
							byteBuffer.putLong(0);
						} else {
							byteBuffer.put((byte) 0x07);
							byteBuffer.putLong(Long.parseLong(value));
						}
						break;
					case "REAL":
						if(value == null) {
							byteBuffer.put((byte) 0x02);
							byteBuffer.putFloat(0);
						} else {
							byteBuffer.put((byte) 0x08);
							byteBuffer.putFloat(Float.parseFloat(value));
						}
						break;
					case "DOUBLE":
						if(value == null) {
							byteBuffer.put((byte) 0x03);
							byteBuffer.putDouble(0);
						} else {
							byteBuffer.put((byte) 0x09);
							byteBuffer.putDouble(Double.parseDouble(value));
						}
						break;
					case "DATETIME":
						if(value == null) {
							byteBuffer.put((byte) 0x03);
							byteBuffer.putDouble(0);
						} else {
							byteBuffer.put((byte) 0x0A);
							SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
							Date value_date = parser.parse(value);
							Calendar calendar = Calendar.getInstance();
							calendar.setTime(value_date);
							long epochSeconds = calendar.getTimeInMillis() / 1000;
							byteBuffer.putLong(epochSeconds);
						}
						break;
					case "DATE":
						if(value == null) {
							byteBuffer.put((byte) 0x03);
							byteBuffer.putDouble(0);
						} else {
							byteBuffer.put((byte) 0x0B);
							SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd");
							Date value_date = parser.parse(value);
							Calendar calendar = Calendar.getInstance();
							calendar.setTime(value_date);
							long epochSeconds = calendar.getTimeInMillis() / 1000;
							byteBuffer.putLong(epochSeconds);
						}
						break;
					case "TEXT":
						if(value == null) {
							byteBuffer.put((byte) 0x0C);
						} else {
							byteBuffer.put((byte) (0x0C + value.length()));
							byteBuffer.put(value.getBytes());
						}
						break;
				}

			} else
				pk = Integer.parseInt(values.get(key));
		}
		byteBuffer.putInt(2, pk);
		return new Record(byteBuffer.array());
	}
	
	public Column getPkColumn() {
		if(this.pkColumn != null)
			return this.pkColumn;
		for(Map.Entry<String, Column> entry : columns.entrySet()) {
			Column column = entry.getValue();
			if(column.isPk()) {
				this.pkColumn = column;
				return this.pkColumn;
			}
		}
		return null;
	}

	public short getRecordLength(LinkedHashMap<String, String> values) {
		short recordLength = 6 + 1;
		for(Map.Entry<String, Column> entry : columns.entrySet()) {
			String key = entry.getKey();
			Column column = entry.getValue();
			if(!column.isPk())
				recordLength += column.getTypeLength(values.get(key)) + 1;
		}
		return recordLength;
	}

	public int writeRecord(RandomAccessFile file, LinkedHashMap<String, String> values, long position) throws IOException, ParseException {
		file.seek(position);
		int recordSize = this.getRecordLength(values) - 6;
		file.writeShort(recordSize);
		int pk = -1;
		file.skipBytes(4);
		file.writeByte(columns.size());
		for(Map.Entry<String, Column> entry : columns.entrySet()) {
			String key = entry.getKey();
			Column column = entry.getValue();
			if(!column.isPk())
				column.writeValue(file, values.get(key));
			else
				pk = Integer.parseInt(values.get(key));
		}
		file.seek(position + 2);
		file.writeInt(pk);
		return recordSize;
	}
	
	public boolean validateValues(LinkedHashMap<String, String> values) {
		for(Map.Entry<String, String> entry : values.entrySet()) {
			if(this.columns.get(entry.getKey()) == null)
				return false;
		}
		for(Map.Entry<String, Column> entry : columns.entrySet()) {
			String key = entry.getKey();
			Column column = entry.getValue();
			if (!column.check(values.get(key))) {
				System.out.println("Invalid " + key + " value : " + values.get(key));
				return false;
			}
		}
		return getRecordLength(values) <= UtilityTools.pageSize - 10;
	}

	public boolean checkCreation() {
		for(Map.Entry<String, Column> entry : columns.entrySet()) {
			Column column = entry.getValue();
			if(!column.checkCreation()) {
				System.out.println(column.toString());
				return false;
			}
		}
		return true;
	}

	public String getFilePath() {
		return "data/" + this.dbName + "/" + this.tableName + ".tbl";
	}
}