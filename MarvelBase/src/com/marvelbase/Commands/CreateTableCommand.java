package com.marvelbase.Commands;

import com.marvelbase.Column;
import com.marvelbase.Table;
import com.marvelbase.UtilityTools;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.marvelbase.MarvelBase.displayError;
import static com.marvelbase.MarvelBase.logMessage;
import static com.marvelbase.UtilityTools.sbTablesTable;

public class CreateTableCommand implements Command {

	private String command = null;
	private String currentDb;

	public CreateTableCommand(String command, String currentDb) {
		this.command = command;
		this.currentDb = currentDb;
	}

	@Override
	public boolean execute() {
		if(command == null) {
			displayError("FATAL ERROR: Command not initialized");
			return false;
		}
		return parseCreateString();
	}
	
	private List<LinkedHashMap<String, String>> getColumnValues(Table table, int pk) {
		List<LinkedHashMap<String, String>> list = new ArrayList<>();
		pk++;
		for (Map.Entry<String, Column> entry : table.getColumns().entrySet()) {
			Column column = entry.getValue();
			list.add(UtilityTools.getColumnsTableRow(column, pk));
			pk++;
		}
		return list;
	}
	
	private boolean createTable(Table table) {
		if(!table.checkCreation()) {
			displayError("Type not supported");
			return false;
		}
		int tablesLastPk = CommandHelper.getLastPk(sbTablesTable);
		int columnsLastPk = CommandHelper.getLastPk(UtilityTools.sbColumnsTable);
		LinkedHashMap<String, String> tableValues = new LinkedHashMap<>();
		tableValues.put("row_id", (tablesLastPk + 1) + "");
		tableValues.put("table_name", table.getTableName());
		tableValues.put("database_name", table.getDbName());
		tableValues.put("record_count", "0");
		tableValues.put("avg_length", "0");
		try {
			RandomAccessFile sBTablesTableFile = new RandomAccessFile("data/catalog/mb_tables.tbl", "rw");
			logMessage("Add Table Row");
			InsertCommand insertCommand = new InsertCommand(new InsertParams(sBTablesTableFile, Integer.parseInt(tableValues.get("row_id")), 0, tableValues, sbTablesTable, null));
			insertCommand.executeTraverseAndInsert();
			sBTablesTableFile.close();
		} catch (ParseException | IOException e) {
			e.printStackTrace();
			return false;
		}
		try {
			RandomAccessFile sBTablesTableFile = new RandomAccessFile("data/catalog/mb_columns.tbl", "rw");
			List<LinkedHashMap<String, String>> columnsList = getColumnValues(table, columnsLastPk);
			for (LinkedHashMap<String, String> values: columnsList) {
				InsertCommand insertCommand = new InsertCommand(new InsertParams(sBTablesTableFile, Integer.parseInt(values.get("row_id")), 0, values, UtilityTools.sbColumnsTable, null));
				insertCommand.executeTraverseAndInsert();
			}
			sBTablesTableFile.close();
		} catch (ParseException | IOException e) {
			e.printStackTrace();
			return false;
		}
		try {
			RandomAccessFile tableFile = new RandomAccessFile(table.getFilePath(), "rw");
			tableFile.setLength(UtilityTools.pageSize);
			tableFile.seek(0);
			tableFile.writeByte(0x0D);
			tableFile.skipBytes(1);
			tableFile.writeShort((int) UtilityTools.pageSize);
			tableFile.writeInt(-1);
			tableFile.close();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		logMessage("Table Created");
		return true;
	}

	public boolean parseCreateString() {
		logMessage("Parsing create table\n" + command);
		Pattern createTablePattern = Pattern.compile("^create table ([^(]*)\\((.*)\\)$");
		Matcher commandMatcher = createTablePattern.matcher(command);
		if(commandMatcher.find()) {
			String dbTableName = commandMatcher.group(1).trim();
			String[] dbTableNameSplit = dbTableName.split("\\.");
			String tableName;
			String dbName;
			if(dbTableNameSplit.length > 1) {
				dbName = dbTableNameSplit[0];
				tableName = dbTableNameSplit[1];
			} else {
				if(this.currentDb == null) {
					displayError("No database selected");
					return false;
				}
				dbName = this.currentDb;
				tableName = dbTableName;
			}
			if(dbName.equalsIgnoreCase("catalog")) {
				displayError("You cannot create table inside catalog.");
				return false;
			}
			File dbFile = new File("data/" + dbName);
			if(!dbFile.exists() || !dbFile.isDirectory()) {
				displayError("Database does not exist.");
				return false;
			}
			String fileName = "data/" + dbName + "/" + tableName + ".tbl";
			File f = new File(fileName);
			if (f.exists()) {
				displayError("Table already exists");
				return false;
			}
			String columnStringPart = commandMatcher.group(2).trim();
			String[] columnsStrings = columnStringPart.split(",");
			List<Column> columns = new ArrayList<>();
			columns.add(new Column("row_id", "INT", false, 1, tableName, dbName, true));
			int position = 2;
			for (String columnString : columnsStrings) {
				columnString = columnString.trim();
				String[] columnTypes = columnString.split(" ", 3);
				if (columnTypes.length < 2) {
					displayError("Couldn't read any column names. Please check your syntax");
					return false;
				}
				columns.add(new Column(columnTypes[0], columnTypes[1], !(columnTypes.length == 3 && columnTypes[2].equals("NOT NULL")), position, tableName, dbName, false));
				position++;
			}
			LinkedHashMap<String, Column> columnsHashMap = new LinkedHashMap<>();
			for (Column column : columns) {
				columnsHashMap.put(column.getName(), column);
			}
			Table table = new Table(dbName, tableName, columnsHashMap);
			return createTable(table);
		} else {
			CommandHelper.wrongSyntax();
			return false;
		}
	}

}
