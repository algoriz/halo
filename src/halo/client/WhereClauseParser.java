package halo.client;

import halo.common.Bytes;
import halo.common.Text;

import java.util.Stack;

/**
 * A recursive parser for WhereClause statement
 */
public class WhereClauseParser {
    /**
     * These constants represent the priorities of the corresponding symbols
     */
    static final int LEFT_BRACKET = 10;
    static final int CONNECTOR_AND = 8;
    static final int CONNECTOR_OR = 7;
    static final int RIGHT_BRACKET = 1;
    static final int EOS = 0;

    /* SQL text */
    String text;

    /* Current position */
    int pos;

    /* Where the parsing should stop at */
    int end;

    WhereClause whereClause;
    ColumnFilterTree root;
    Stack<ColumnFilterTree> nodes;
    Stack<Integer> operators;

    public WhereClauseParser(String text, int startPos, int stopPos) throws BadSqlStatement {
        this.text = text;
        this.pos = startPos;
        this.end = stopPos;

        root = new ColumnFilterTree();
        nodes = new Stack<ColumnFilterTree>();
        operators = new Stack<Integer>();

        skipSpaces();
        if (!getSequence().toUpperCase().equals("WHERE")) {
            throw new BadSqlStatement("Not a where clause statement");
        }
        parse();
    }

    private static boolean isSpace(char ch) {
        return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n';
    }

    private static boolean isAlpha(char ch) {
        return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z');
    }

    private static boolean isDigit(char ch) {
        return ch >= '0' && ch <= '9';
    }

    public WhereClause getWhereClause() {
        return whereClause;
    }

    private boolean isSpace() {
        return isSpace(text.charAt(pos));
    }

    private boolean isSeparator() {
        char ch = text.charAt(pos);
        return !(isAlpha(ch) || isDigit(ch) || ch == '_');
    }

    private void skipSpaces() {
        while (pos < end && isSpace()) {
            ++pos;
        }
    }

    private boolean upperEquals(String upperCaseString) {
        return text.substring(pos, pos + upperCaseString.length())
                .toUpperCase().equals(upperCaseString);
    }

    private char getChar() {
        return text.charAt(pos++);
    }

    private String getSequence() {
        skipSpaces();
        StringBuilder sb = new StringBuilder();
        while (pos < end && !isSeparator()) {
            sb.append(getChar());
        }
        return sb.toString();
    }

    private int getOperator() throws BadSqlStatement {
        skipSpaces();
        StringBuilder builder = new StringBuilder();
        while (pos < end) {
            char ch = text.charAt(pos);
            /* Accepts the following characters only */
            if (ch == '!' || ch == '>' || ch == '<' || ch == '=') {
                builder.append(ch);
                ++pos;
            } else break;
        }

        try {
            return ColumnFilterOperator.valueOf(builder.toString());
        } catch (Exception e) {
            throw new BadSqlStatement("Unknown operator \"" + builder.toString() + "\".");
        }
    }

    private byte[] getValue() throws BadSqlStatement {
        /**
         * A value is either a sequence or a sequence quoted by a pair
         * of quotation marks
         */
        skipSpaces();
        if (text.charAt(pos) == '"') {
            try {
                Text.QuotedStringResult result = Text.readQuotedString(text, pos, end);
                pos = result.stopPosition;
                return Bytes.toBytes(result.string);
            } catch (Exception e) {
                throw new BadSqlStatement(e.getMessage());
            }
        }
        return Bytes.toBytes(getSequence());
    }

    private ColumnFilter getFilter() throws BadSqlStatement {
        skipSpaces();
        boolean bracketed = text.charAt(pos) == '(';
        if (bracketed) {
            ++pos;
        }

        ColumnFilter columnFilter;
        String column = getSequence();
        int lpos = pos;
        String nextToken = getSequence().toUpperCase();
        if (nextToken.equals("BETWEEN")){
            /* column BETWEEN startValue AND stopValue */
            BetweenAndArguments arguments = new BetweenAndArguments();
            arguments.setStartValue(getValue());
            if (!getSequence().toUpperCase().equals("AND")){
                throw new BadSqlStatement("Invalid BETWEEN-AND syntax, expecting keyword AND.");
            }
            arguments.setStopValue(getValue());
            columnFilter = new ColumnFilter(column, ColumnFilterOperator.BETWEEN_AND, arguments.toBytes());
        }
        else {
            /* column BINARY-OP value */
            pos = lpos;
            columnFilter = new ColumnFilter(column, getOperator(), getValue());
        }

        if (bracketed) {
            skipSpaces();
            if (text.charAt(pos) != ')') {
                throw new BadSqlStatement("Missing right bracket at position " + pos);
            }
            ++pos;
        }
        return columnFilter;
    }

    private void pushOperator(int current) throws BadSqlStatement {
        if (operators.isEmpty() || operators.peek() < current) {
            operators.push(current);
        } else {
            ColumnFilterTree left, right;
            int prev = operators.peek();
            switch (prev) {
                case LEFT_BRACKET:
                    if (current == RIGHT_BRACKET) {
                        operators.pop();
                    } else {
                        operators.push(current);
                    }
                    break;
                case CONNECTOR_AND:
                    if (nodes.size() < 2) {
                        throw new BadSqlStatement("Bad AND operator usage");
                    }
                    right = nodes.peek();
                    nodes.pop();
                    left = nodes.peek();
                    nodes.pop();
                    nodes.push(new ColumnFilterTree(left, ColumnFilterTree.AND, right));
                    operators.pop();
                    pushOperator(current);
                    break;
                case CONNECTOR_OR:
                    if (nodes.size() < 2) {
                        throw new BadSqlStatement("Bad OR operator usage");
                    }
                    right = nodes.peek();
                    nodes.pop();
                    left = nodes.peek();
                    nodes.pop();
                    nodes.push(new ColumnFilterTree(left, ColumnFilterTree.OR, right));
                    operators.pop();
                    pushOperator(current);
                    break;
                case RIGHT_BRACKET:
                    throw new BadSqlStatement("Bracket mismatch");
                default:
                    throw new BadSqlStatement("Unknown operator");
            }
        }
    }

    private void parse() throws BadSqlStatement {
        boolean semicolonTerminated = false;
        while (pos < end) {
            skipSpaces();
            if (pos >= end) {
                break;
            }

            char ch = text.charAt(pos);
            if (ch == '(') {
                ++pos;
                pushOperator(LEFT_BRACKET);
            } else if (ch == ')') {
                ++pos;
                pushOperator(RIGHT_BRACKET);
            } else if (ch == ';') {
                ++pos;
                semicolonTerminated = true;
                pushOperator(EOS);
                break;
            } else {
                int lpos = pos;
                String nextToken = getSequence().toUpperCase();
                if (nextToken.equals("AND")) {
                    pushOperator(CONNECTOR_AND);
                } else if (nextToken.equals("OR")) {
                    pushOperator(CONNECTOR_OR);
                } else {
                    pos = lpos;
                    nodes.push(new ColumnFilterTree(getFilter()));
                }
            }
        }

        if (!semicolonTerminated) {
            /* Pseudo operator: End Of Statement */
            pushOperator(EOS);
        }

        if (operators.size() != 1 || nodes.size() != 1) {
            throw new BadSqlStatement("Unexpected end of where clause statement");
        }

        whereClause = new WhereClause(nodes.peek());
    }
}
