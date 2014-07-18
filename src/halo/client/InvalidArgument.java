package halo.client;

/**
 * Represents an exception that is caused by an invalid argument.
 */
public class InvalidArgument extends Exception {
    public InvalidArgument(String message){
        super(message);
    }
}
