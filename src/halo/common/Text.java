package halo.common;

import halo.client.InvalidArgument;

/**
 * A tool class to handle text processing
 */
public final class Text {
    public static class QuotedStringResult {
        /* The quoted string, excluding the leading and trailing quotation mark. */
        public String string;
        /* Where the quoted string ends. */
        public int startPosition;
        /* Where the quoted string ends. */
        public int stopPosition;
    }

    public static QuotedStringResult readQuotedString(
            String text) throws InvalidArgument {
        return readQuotedString(text, 0, text.length());
    }

    public static QuotedStringResult readQuotedString(
            String text, int start) throws InvalidArgument {
        return readQuotedString(text, start, text.length());
    }

    /**
     * Extracts an quoted string from the given text.
     * The escape sequences within the quoted string are converted to corresponding
     * characters.
     *
     * The following escape sequences are supported:
     *   \0, \b, \n, \r, \t, \", \\
     *
     *  If text[start, stop) doesn't contains a valid quoted string, an InvalidArgument
     * exception is thrown.
     *
     * @param text Text that contains the quoted string.
     * @param start Where the parsing starts from.
     * @param stop Where the parsing stops at. The parsing may stop before this position.
     * @throws InvalidArgument
     */
    public static QuotedStringResult readQuotedString(
            String text, int start, int stop) throws InvalidArgument {
        QuotedStringResult result = new QuotedStringResult();

        int pos = start;
        while (text.charAt(pos) != '"' && pos < stop) {
            ++pos;
        }
        result.startPosition = pos;
        ++pos;

        boolean escaped = false;
        StringBuilder builder = new StringBuilder();
        while (pos < stop) {
            char ch = text.charAt(pos++);
            if (!escaped && ch != '\\' && ch != '"') {
                builder.append(ch);
                continue;
            }

            switch (ch) {
                case '\\':
                    if (escaped) {
                        builder.append(ch);
                    }
                    escaped = !escaped;
                    break;
                case '"':
                    if (escaped) {
                        escaped = false;
                        builder.append(ch);
                    } else {
                        /* Finish capturing */
                        result.string = builder.toString();
                        result.stopPosition = pos;
                        return result;
                    }
                    break;
                case '0':
                    escaped = false;
                    builder.append('\0');
                    break;
                case 'b':
                    escaped = false;
                    builder.append('\b');
                    break;
                case 'n':
                    escaped = false;
                    builder.append('\n');
                    break;
                case 'r':
                    escaped = false;
                    builder.append('\r');
                    break;
                case 't':
                    escaped = false;
                    builder.append('\t');
                    break;
                default:
                    throw new InvalidArgument("Unsupported escape character [\\" + ch + "]");
            }
        }
        throw new InvalidArgument("Unexpected end of string.");
    }
}
