package halo.core;

import halo.common.Bytes;
import halo.common.ColumnSpecifierHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Provides methods for data manipulation for a single Halo table.
 */
public class HaloTable {
    static class ColumnSpecifierFormatter implements ColumnSpecifierHelper.Formatter {
        @Override
        public byte[] format(int column) {
            return Integer.toString(column).getBytes();
        }
    }

    static ColumnSpecifierHelper colspec =
            new ColumnSpecifierHelper(new ColumnSpecifierFormatter(), 32);

    private TableProperty tableProperty;
    private Configuration conf;
    private HTable primaryTable;
    private HTable[] indexTables;

    public HaloTable(Configuration conf, TableProperty tableProperty) throws IOException {
        this.conf = conf;
        this.tableProperty = tableProperty;

        int columns = tableProperty.getNumberOfColumns();
        primaryTable = new HTable(conf, tableProperty.getPrimaryTableName());
        indexTables = new HTable[columns];
        for (int i = 0; i < columns; ++i) {
            if (tableProperty.getColumnProperty(i).isIndex()) {
                indexTables[i] = new HTable(conf, tableProperty.getIndexTableName(i));
            }
        }

        /**
         * We do NOT book next row ID or total number of rows of the
         * table in the metadata table.
         * NextRowId is determined by binary search through row id
         * space every time opening the table.
         *
         * TODO:
         * Some optimization may be possible by providing a probe hint
         */
        tableProperty.setNextRowId(findRowIdUpperBound());
    }

    public NonQueryResult insert(int[] selectedCols, byte[][] values) throws IOException {
        if (selectedCols.length != values.length) {
            throw new IOException("Number of columns and values mismatch");
        }

        long rowId = tableProperty.getNextRowId();
        tableProperty.setNextRowId(rowId + 1);

        int columns = tableProperty.getNumberOfColumns();
        byte[][] orderedValues = new byte[columns][];
        for (int i = 0; i < columns; ++i) {
            orderedValues[i] = Bytes.NULL;
        }
        reorderValues(selectedCols, values, orderedValues);

        putValues(RowId.valueOf(rowId), orderedValues);

        NonQueryResult nonQueryResult = new NonQueryResult();
        nonQueryResult.setRowsAffected(1);
        return nonQueryResult;
    }

    public NonQueryResult update(
            int[] selectedCols, byte[][] values, RowSet target) throws IOException {
        if (selectedCols.length != values.length) {
            throw new IOException("Number of columns and values mismatch");
        }

        byte[][] orderedValues = new byte[tableProperty.getNumberOfColumns()][];
        for (long r = 0; r < target.size(); ++r) {
            byte[] rowid = target.get(r);
            reorderValues(selectedCols, values, orderedValues);
            putValues(rowid, orderedValues);
        }

        NonQueryResult nonQueryResult = new NonQueryResult();
        nonQueryResult.setRowsAffected(target.size());
        return nonQueryResult;
    }

    private byte[][] reorderValues(int[] columns, byte[][] values, byte[][] orderedValues) {
        for (int i = 0; i < columns.length; ++i) {
            orderedValues[columns[i]] = values[i];
        }
        return orderedValues;
    }

    public NonQueryResult delete(RowSet target) throws IOException {
        for (long r = 0; r < target.size(); ++r) {
            byte[] rowKey = target.get(r);
            Result result = primaryTable.get(new Get(rowKey));

            Delete del = new Delete(rowKey);
            primaryTable.delete(del);

            int columns = tableProperty.getNumberOfColumns();
            for (int i = 0; i < columns; ++i) {
                if (tableProperty.getColumnProperty(i).isIndex()) {
                    indexTables[i].delete(
                            new Delete(Bytes.concat(getColumnValue(result, i), rowKey)));
                }
            }
        }
        NonQueryResult result = new NonQueryResult();
        result.setRowsAffected(target.size());
        return result;
    }

    public QueryResult select(int[] selectedCols, RowSet target) throws IOException {
        QueryResult queryResult = new QueryResult(selectedCols.length);
        for (long r = 0; r < target.size(); ++r) {
            byte[][] values = queryValues(target.get(r), selectedCols);
            if (values != null) {
                queryResult.add(values);
            }
        }
        return queryResult;
    }

    /**
     * Write
     * @param rowid
     * @param values
     * @throws IOException
     */
    private void putValues(byte[] rowid, byte[][] values) throws IOException {
        Put primaryPut = new Put(rowid);
        int columns = tableProperty.getNumberOfColumns();
        for (int i = 0; i < columns; ++i) {
            byte[] value = values[i];
            if (value == null) {
                continue;
            }

            ColumnProperty prop = tableProperty.getColumnProperty(i);
            primaryPut.add(HaloAdmin.PRIMARY_FAMILY, colspec.get(i), value);
            if (prop.isIndex()) {
                /**
                 * Update index record
                 * RowKey = value + filled_zeros + value_len(4Byte) + rowid(8Byte)
                 */
                ByteBuffer buffer = ByteBuffer.allocate(prop.getMaxLength() + 12);
                buffer.put(value);
                buffer.position(prop.getMaxLength());
                buffer.putInt(value.length).put(rowid);
                Put put = new Put(buffer.array());
                put.add(HaloAdmin.INDEX_FAMILY, Bytes.NULL, Bytes.NULL);
                indexTables[i].put(put);
            }
        }

        primaryTable.put(primaryPut);
    }

    private byte[][] queryValues(byte[] row, int[] selectedCols) throws IOException {
        int columns = selectedCols.length;
        Get get = new Get(row);
        for (int i = 0; i < columns; ++i) {
            get.addColumn(HaloAdmin.PRIMARY_FAMILY, colspec.get(selectedCols[i]));
        }

        Result result = primaryTable.get(get);
        if (!result.isEmpty()) {
            byte[][] values = new byte[columns][];
            for (int i = 0; i < columns; ++i) {
                values[i] = getColumnValue(result, selectedCols[i]);
            }
            return values;
        }
        return null;
    }

    public void close() {
        try {
            primaryTable.close();
        } catch (Exception e) {
        }
        for (HTable t : indexTables) {
            if (t != null) {
                try {
                    t.close();
                } catch (Exception e) {
                }
            }
        }
    }

    public TableProperty getTableProperty() {
        return tableProperty;
    }

    public RowSet scan(ColumnScans columnScans) throws IOException {
        if (columnScans.getColumn() >= tableProperty.getNumberOfColumns()) {
            throw new IOException("Column #" + columnScans.getColumn()
                    + " out of range, table(" + tableProperty.getName() + ")");
        }

        HTable indexTable = indexTables[columnScans.getColumn()];
        RowSet rows = new RowSet();
        for (ColumnScans.Segment segment : columnScans.getSegments()) {
            ResultScanner scanner = indexTable.getScanner(segment.toScan());
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                rows.add(RowId.fromIndexRow(result.getRow()));
            }
        }
        return rows;
    }

    public RowSet scanPrimary(Scan scan) throws IOException {
        RowSet rows = new RowSet();
        ResultScanner scanner = primaryTable.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            rows.add(result.getRow());
        }
        return rows;
    }

    public int findColumn(String columnLabel) {
        return tableProperty.findColumn(columnLabel);
    }

    private byte[] getColumnValue(Result result, int column) {
        return result.getValue(HaloAdmin.PRIMARY_FAMILY, colspec.get(column));
    }

    /**
     * Get the maximum RowId of current table.
     *
     * @return
     * @throws IOException
     */
    private long findRowIdUpperBound() throws IOException {
        long lower = 0;
        long upper = RowId.UPPER_ID;
        long probe = upper / 2;
        while (upper - lower > 1) {
            if (rowExists(probe)) {
                lower = probe;
                /**
                 * NEVER write code like:
                 *   probe = (upper + probe) / 2;
                 * That will cause OVERFLOW!!!
                 */
                probe += (upper - probe) / 2;
            } else {
                upper = probe;
                /* SAME AS ABOVE */
                probe -= (probe - lower) / 2;
            }
        }
        return upper;
    }

    private boolean rowExists(long startRowId) throws IOException {
        Scan scan = new Scan(Bytes.toBytes(startRowId));
        return primaryTable.getScanner(scan).next() != null;
    }
}
