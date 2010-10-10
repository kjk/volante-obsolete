package org.nachodb;

/**
 * Exception thrown during import of data from XML file in database
 */
public class XMLImportException extends Exception { 
    public XMLImportException(int line, int column, String message) { 
        super("In line " + line + " column " + column + ": " + message);
        this.line = line;
        this.column = column;
        this.message = message;
    }

    public String getMessageText() { 
        return message;
    }
    
    public int getLine() { 
        return line;
    }

    public int getColumn() { 
        return column;
    }

    private int line;
    private int column;
    private String message;
}
