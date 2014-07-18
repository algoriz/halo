package halo.client;

import com.sun.tools.corba.se.idl.InvalidArgument;

/**
 * The operator used in a ColumnFilter object.
 */
public final class ColumnFilterOperator {
    public static final int LESS = 1;
    public static final int LESS_OR_EQUAL = 2;
    public static final int EQUAL = 3;
    public static final int NOT_EQUAL = 4;
    public static final int GREATER_OR_EQUAL = 5;
    public static final int GREATER = 6;
    public static final int BETWEEN_AND = 7;

    /**
     * Parsing an binary operator from string.
     * NOTE that non-binary operators can not be handled by this method.
     * @param op String to be parsed
     * @return The ID of the binary operator
     * @throws InvalidArgument In case op is not a valid binary operator
     */
    public static int valueOf(String op) throws InvalidArgument {
        if (op.length() == 1) {
            if (op.equals("<")) {
                return ColumnFilterOperator.LESS;
            }
            if (op.equals("=")) {
                return ColumnFilterOperator.EQUAL;
            }
            if (op.equals(">")) {
                return ColumnFilterOperator.GREATER;
            }
        } else if (op.length() == 2) {
            if (op.equals(">=")) {
                return ColumnFilterOperator.GREATER_OR_EQUAL;
            } else if (op.equals("!=")) {
                return ColumnFilterOperator.NOT_EQUAL;
            } else if (op.equals("<=")) {
                return ColumnFilterOperator.LESS_OR_EQUAL;
            } else if (op.equals("==")) {
                return ColumnFilterOperator.EQUAL;
            }
        }
        throw new InvalidArgument("Unknown binary filter operator: " + op);
    }
}
