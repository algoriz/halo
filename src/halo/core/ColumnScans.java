package halo.core;

import halo.common.Bytes;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Scan;

public class ColumnScans {
    public static class Segment {
        private byte[] start;
        private byte[] stop;

        public Segment(byte[] start, byte[] stop) {
            this.start = start;
            this.stop = stop;
        }

        /**
         * Merge another segment into this segment using OR semantics.
         *
         * @param segment The segment to be merged.
         * @return this
         */
        public Segment union(Segment segment) {
            if (start == null || segment.start == null) {
                start = null;
            } else {
                start = Bytes.minOf(start, segment.start);
            }

            if (stop == null || segment.stop == null) {
                stop = null;
            } else {
                stop = Bytes.maxOf(stop, segment.stop);
            }
            return this;
        }

        /**
         * Merge another segment into this segment using AND semantics.
         *
         * @param segment The segment to be merged.
         * @return this
         */
        public Segment intersect(Segment segment) {
            if (start != null && segment.start != null) {
                start = Bytes.maxOf(start, segment.start);
            } else {
                start = start != null ? start : segment.start;
            }

            if (stop != null && segment.stop != null) {
                stop = Bytes.minOf(stop, segment.stop);
            } else {
                stop = stop != null ? stop : segment.stop;
            }
            return this;
        }

        /**
         * Tests whether this segment is an empty segment, i.e. logically start >= stop.
         * Note that null value for start or stop doesn't mean zero value, instead it
         * means negative infinite or positive infinite correspondingly.
         */
        public boolean isEmpty() {
            return start != null && stop != null && Bytes.compare(start, stop) >= 0;
        }

        /**
         * Tests whether this segment is placed at the left of another segment
         */
        public boolean atLeftOf(Segment segment) {
            return stop != null && segment.start != null && Bytes.compare(stop, segment.start) < 0;
        }

        /**
         * Tests whether this segment is placed at the right of another segment.
         */
        public boolean atRightOf(Segment segment) {
            return start != null && segment.stop != null && Bytes.compare(start, segment.stop) > 0;
        }

        public Scan toScan() {
            return new Scan(start, stop);
        }

        public byte[] getStop() {
            return stop;
        }

        public void setStop(byte[] stop) {
            this.stop = stop;
        }

        public byte[] getStart() {
            return start;
        }

        public void setStart(byte[] start) {
            this.start = start;
        }
    }

    private int column;
    private ArrayList<Segment> segments;

    public ColumnScans(int column) {
        this.column = column;
        segments = new ArrayList<Segment>();
    }

    /* Gets the index of the column that this ColumnScans is bound to. */
    public int getColumn() {
        return column;
    }

    public ArrayList<Segment> getSegments() {
        return segments;
    }

    public ColumnScans add(byte[] startRow, byte[] stopRow) throws IOException {
        Segment segment = new Segment(startRow, stopRow);
        int lower = 0;
        int upper = segments.size();
        while (lower + 1 < upper) {
            int probe = (lower + upper) / 2;
            Segment target = segments.get(probe);
            if (segment.atLeftOf(target)) {
                upper = probe;
            } else if (segment.atRightOf(target)) {
                lower = probe;
            } else {
                /* The added segment intersects with one of existing segment. */
                target.union(segment);
                return this;
            }
        }
        segments.add(upper, segment);
        return this;
    }

    /* Test whether another ColumnScans can be merged with this ColumnScans. */
    public boolean isMergable(ColumnScans another) {
        return column == another.column;
    }

    public boolean isEmpty() {
        return segments.size() == 0;
    }

    /**
     * Merges another ColumnScans into this ColumnScans using OR semantics
     *
     * @param another The ColumnScans to be merged.
     * @return this
     * @throws IOException Throws IOException if another ColumnScans object is not mergable with this object.
     */
    public ColumnScans unionMerge(ColumnScans another) throws IOException {
        if (!isMergable(another)) {
            throw new IOException("ColumnScans on different column can't be merged.");
        }

        if (another.isEmpty() || this == another) {
            return this;
        }

        if (isEmpty()) {
            segments = (ArrayList<Segment>) another.segments.clone();
            return this;
        }

        ArrayList<Segment> a = segments;
        ArrayList<Segment> b = another.segments;
        segments = new ArrayList<Segment>(a.size() + b.size());
        int i = 0, na = a.size();
        int j = 0, nb = b.size();
        if (Bytes.compare(a.get(0).getStart(), b.get(0).getStart()) < 0) {
            ++i;
            segments.add(a.get(0));
        } else {
            ++j;
            segments.add(b.get(0));
        }

        while (i < na && j < nb) {
            Segment l = a.get(i);
            Segment r = b.get(j);
            Segment last = segments.get(segments.size()-1);
            if (l.atLeftOf(r) && l.atRightOf(last)){
                ++i;
                segments.add(l);
            } else if (r.atLeftOf(l)){
                ++j;
                segments.add(r);
            } else {
            }
        }
        return this;
    }

    public ColumnScans intersectMerge(ColumnScans another) throws IOException {
        if (!isMergable(another)) {
            throw new IOException("ColumnScans on different column can't be merged.");
        }

        if (this.isEmpty() || this == another) {
            return this;
        }

        if (another.isEmpty()) {
            segments.clear();
            return this;
        }

        int p = 0;
        for (int i = 0; i < another.segments.size(); ++i) {
            Segment target = another.segments.get(i);
            while (p < segments.size() && segments.get(p).atLeftOf(target)) {
                segments.remove(p);
            }

            if (p >= segments.size()) {
                break;
            }

            if (!segments.get(p).atRightOf(target)) {
                segments.get(p).intersect(target);
                ++p;
            }
        }
        return this;
    }
}
