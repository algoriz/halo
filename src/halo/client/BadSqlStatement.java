package halo.client;

/**
 * Represents an expcetion that is caused by a bad SQL statement.
 */
public class BadSqlStatement extends Exception {
    public BadSqlStatement(String message){
        super(message);
    }
}
