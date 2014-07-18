package halo.common;


/**
 * A tool class to help generate and cache column specifiers.
 * Since column specifiers are frequently used by low-layer code,
 * it's a better strategy to generate once and reuse.
 */
public class ColumnSpecifierHelper {
    public interface Formatter{
        public byte[] format(int column);
    }

    Formatter formatter;
    byte[][] cache;

    /**
     * Constructs a ColumnSpecifierHelper.
     * @param formatter The formatter used to generate column specifier
     * @param initCapacity initial number of colum specifiers to generate.
     */
    public ColumnSpecifierHelper(Formatter formatter, int initCapacity){
        if (initCapacity <= 0){
            initCapacity = 16;
        }
        this.formatter = formatter;
        makeCache(initCapacity);
    }

    private void makeCache(int size){
        byte[][] old = cache;
        int oldLength = old == null ? 0 : old.length;
        cache = new byte[size][];
        for (int i = 0; i < oldLength; ++i){
            cache[i] = old[i];
        }
        for (int i = oldLength; i < size; ++i){
            cache[i] = formatter.format(i);
        }
    }

    /**
     * Gets the column specifier for the column.
     * @param column column index
     * @return A byte array represented column specifier.
     */
    public byte[] get(int column){
        if (column >= cache.length){
            makeCache(cache.length * 2);
        }
        return cache[column];
    }
}
