package com.marvelbase.Commands;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.marvelbase.MarvelBase.displayError;
import static com.marvelbase.MarvelBase.logMessage;
import static com.marvelbase.MarvelBase.response;

public class DropDatabaseCommand implements Command {

	private String command = null;
	private String deletedDb;

	public DropDatabaseCommand(String command) {
		this.command = command;
	}

	public String getDeletedDb() {
		return deletedDb;
	}

	@Override
	public boolean execute() {
		if(this.command == null) {
			displayError("Command not initialized");
			return false;
		}
		return parseDropDatabaseString();
	}

	private boolean parseDropDatabaseString() {
		logMessage("Calling create on the query\n" + this.command);
		Pattern createTablePattern = Pattern.compile("^drop database ([a-z][a-z0-9]*)$");
		Matcher commandMatcher = createTablePattern.matcher(this.command);
		if(commandMatcher.find()) {
			String dbName = commandMatcher.group(1).trim();
			if(dbName.equalsIgnoreCase("catalog")) {
				displayError("You cannot drop catalog database.");
				return false;
			}
			//File dbFile = new File("Database/" + dbName);
			File dbFile = new File("data/" + dbName);
			if(!dbFile.exists() || !dbFile.isDirectory()) {
				displayError("Database does not exist.");
				return false;
			}
			if(dbFile.delete()) {
				response("Database " + dbName + " dropped.");
				this.deletedDb = dbName;
				return true;
			} else {
				displayError("Something went wrong deleting the file.");
				return false;
			}
		} else {
			CommandHelper.wrongSyntax();
			return false;
		}
	}

}
