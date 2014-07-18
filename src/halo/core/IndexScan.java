package halo.core;

import halo.common.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A list of RowRange
 */
public class IndexScan{
    public class Range {
        private byte[] startRow;
        private byte[] stopRow;

        public Range() {
        }

        public Range(byte[] startRow, byte[] stopRow) {
            setStartRow(startRow);
            setStopRow(stopRow);
        }

        public byte[] getStartRow() {
            return startRow;
        }

        public byte[] getStopRow() {
            return stopRow;
        }

        public void setStartRow(byte[] startRow) {
            this.startRow = startRow;
        }

        public void setStopRow(byte[] stopRow) {
            this.stopRow = stopRow;
        }

        boolean intersectsWith(Range another) {
            int start = Bytes.compare(stopRow, another.startRow);
            int stop = Bytes.compare(startRow, another.stopRow);
            return start <= 0 || stop >= 0;
        }
    }

    int column;
    ArrayList<Range> ranges;

    public IndexScan(int column) {
        this.column = column;
        ranges = new ArrayList<Range>();
    }

    public int getColumn() {
        return column;
    }

    public ArrayList<Range> getRanges() {
        return ranges;
    }

    public IndexScan add(Range range) {
        if (ranges.isEmpty()){
            ranges.add(range);
        } else {

        }
        return this;
    }

    /* Test whether two scans can be merged */
    public boolean isMergable(IndexScan another){
        return column == another.column;
    }

    public IndexScan merge(IndexScan another) throws IOException {
        if (!isMergable(another)){
            throw new IOException("These two IndexScan can't be merged.");
        }


        return this;
    }
}
