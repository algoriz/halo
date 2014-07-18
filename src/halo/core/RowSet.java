package halo.core;

import halo.common.Bytes;

import java.util.*;

/**
 *
 */
public class RowSet {
    /* The maximum number of rows that may be bufferred */
    public static final long BUFFER_SIZE = 0x10000;

    private ArrayList<byte[]> rowBuffer;
    private long totalRows;
    private long bufferStart;
    private boolean isSorted;

    public RowSet() {
        totalRows = 0;
        bufferStart = 0;
        rowBuffer = new ArrayList<byte[]>((int) BUFFER_SIZE);
        isSorted = false;
    }

    public long size() {
        return totalRows;
    }

    public void add(byte[] rowKey) {
        rowBuffer.add(rowKey);
        ++totalRows;
        isSorted = false;
    }

    public void sort() {
        rowBuffer.sort(RowId.Comparator);
        isSorted = true;
    }

    public byte[] get(long i) {
        return rowBuffer.get((int) (i - bufferStart));
    }

    public RowSet union(RowSet rows) {
        if (!isSorted) {
            sort();
        }
        if (!rows.isSorted) {
            sort();
        }

        RowSet result = new RowSet();
        result.isSorted = true;
        ArrayList<byte[]> merged = result.rowBuffer;
        ArrayList<byte[]> left = rowBuffer;
        ArrayList<byte[]> right = rows.rowBuffer;
        int lend = left.size();
        int rend = right.size();
        int l = 0, r = 0;
        while (l < lend && r < rend) {
            byte[] lv = left.get(l);
            byte[] rv = right.get(r);
            int diff = Bytes.compare(lv, rv);
            if (diff < 0) {
                merged.add(lv);
                ++l;
            } else if (diff > 0) {
                merged.add(rv);
                ++r;
            } else {
                merged.add(lv);
                ++l;
                ++r;
            }
        }

        while (l < lend){
            merged.add(left.get(l));
            ++l;
        }
        while (r < rend){
            merged.add(right.get(r));
            ++r;
        }
        result.totalRows = merged.size();
        return result;
    }

    public RowSet intersect(RowSet rows) {
        if (!isSorted) {
            sort();
        }
        if (!rows.isSorted) {
            sort();
        }

        RowSet result = new RowSet();
        result.isSorted = true;
        ArrayList<byte[]> merged = result.rowBuffer;
        ArrayList<byte[]> left = rowBuffer;
        ArrayList<byte[]> right = rows.rowBuffer;
        int lend = left.size();
        int rend = right.size();
        int l = 0, r = 0;
        while (l < lend && r < rend) {
            byte[] lv = left.get(l);
            byte[] rv = right.get(r);
            int diff = Bytes.compare(lv, rv);
            if (diff < 0) {
                ++l;
            } else if (diff > 0) {
                ++r;
            } else {
                merged.add(lv);
                ++l;
                ++r;
            }
        }

        result.totalRows = merged.size();
        return result;
    }
}
