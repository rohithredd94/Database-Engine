package com.marvelbase.Commands;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.marvelbase.MarvelBase.displayError;
import static com.marvelbase.MarvelBase.logMessage;
import static com.marvelbase.MarvelBase.response;
import static com.marvelbase.UtilityTools.sbColumnsTable;
import static com.marvelbase.UtilityTools.sbTablesTable;

public class DropTableCommand implements Command {

	private String command = null;
	private String currentDb;

	public DropTableCommand(String command, String currentDb) {
		this.command = command;
		this.currentDb = currentDb;
	}

	@Override
	public boolean execute() {
		if(this.command == null) {
			displayError("Command not initialized");
			return false;
		}
		return parseDropString();
	}

	private boolean parseDropString() {
		Pattern pattern = Pattern.compile("^drop table ([^(\\s]*)$");
		Matcher matcher = pattern.matcher(this.command);
		if(matcher.find()) {
			if(matcher.groupCount() == 1) {
				return checkAndExecuteDropCommand(matcher.group(1).trim());
			} else {
				CommandHelper.wrongSyntax();
				return false;
			}
		} else {
			CommandHelper.wrongSyntax();
			return false;
		}
	}


	private boolean checkAndExecuteDropCommand(String dbTableName) {
		logMessage("Executing drop command");
		dbTableName = dbTableName.trim();
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
			displayError("You cannot delete records inside catalog.");
			return false;
		}
		File dbFile = new File("data/" + dbName);
		if(!dbFile.exists() || !dbFile.isDirectory()) {
			displayError("Database does not exist.");
			return false;
		}

		File f = new File("data/" + dbName + "/" + tableName + ".tbl");
		if (!f.exists()) {
			displayError("Table does not exist");
			return false;
		}
		try {
			RandomAccessFile sBTablesTableFile = new RandomAccessFile("data/catalog/mb_tables.tbl", "rw");
			new DeleteCommand().traverseAndDeleteCatalog(sBTablesTableFile, 0, sbTablesTable, tableName, dbName);
			sBTablesTableFile.close();
			RandomAccessFile sBColumnsTableFile = new RandomAccessFile("data/catalog/mb_columns.tbl", "rw");
			new DeleteCommand().traverseAndDeleteCatalog(sBColumnsTableFile, 0, sbColumnsTable, tableName, dbName);
			sBColumnsTableFile.close();
			if(f.delete()) {
				response("Successfully dropped the table " + tableName);
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
}
