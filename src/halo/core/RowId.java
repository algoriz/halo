package halo.core;

import halo.common.Bytes;


/**
 * A tool class to help manipulate RowId's
 */
public class RowId {
    public static class Comparator implements java.util.Comparator<byte[]> {
        public int compare(byte[] o1, byte[] o2) {
            return Bytes.compare(o1, o2);
        }
    }

    public static long LOWER_ID = 1;
    public static long UPPER_ID = Long.MAX_VALUE;
    public static byte[] LOWER_KEY = Bytes.toBytes(LOWER_ID);
    public static byte[] UPPER_KEY = Bytes.toBytes(UPPER_ID);
    public static final Comparator Comparator = new Comparator();

    public static byte[] valueOf(long rowId){
        return Bytes.toBytes(rowId);
    }

    public static long valueOf(byte[] rowId){
        return Bytes.toLong(rowId);
    }

    /**
     * Extract a RowId from an index record.
     */
    public static byte[] fromIndexRow(byte[] row) {
        return Bytes.range(row, row.length-8, 8);
    }
}
