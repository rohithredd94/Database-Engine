package com.marvelbase.Commands;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.marvelbase.MarvelBase.displayError;
import static com.marvelbase.MarvelBase.logMessage;
import static com.marvelbase.MarvelBase.response;

public class CreateDatabaseCommand implements Command {

	private String command = null;

	public CreateDatabaseCommand(String command) {
		this.command = command;
	}

	@Override
	public boolean execute() {
		if(command == null) {
			displayError("Command not initialized");
			return false;
		}
		return parseCreateDatabaseString();
	}

	private boolean parseCreateDatabaseString() {
		logMessage("Creating database\n" + this.command);
		Pattern createTablePattern = Pattern.compile("^create database ([a-z][a-z0-9]*)$");
		Matcher commandMatcher = createTablePattern.matcher(this.command);
		if(commandMatcher.find()) {
			String dbName = commandMatcher.group(1).trim();
			if(dbName.equalsIgnoreCase("catalog")) {
				displayError("You cannot create catalog database.");
				return false;
			}
			File dbFile = new File("data/" + dbName);
			if(dbFile.exists() && dbFile.isDirectory()) {
				displayError("Database already exists.");
				return false;
			}
			if(dbFile.mkdir()) {
				response("Database " + dbName + " created");
			} else {
				displayError("FATAL ERROR: Unable to create database.");
				return false;
			}
		} else {
			CommandHelper.wrongSyntax();
			return false;
		}
		return true;
	}
}
