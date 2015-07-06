package ds.bplus.util;

/**
 * Just a wrapper to state exception.
 */

public class InvalidBTreeStateException extends Exception {
    private static final long serialVersionUID = 7295144377433447079L;
    public InvalidBTreeStateException(String m)
        {super(m);}
}
