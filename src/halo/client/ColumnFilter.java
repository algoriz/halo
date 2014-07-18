package halo.client;

import halo.common.Bytes;
import halo.core.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;

import java.io.IOException;

/**
 * Represents a filter condition that can be applied to an index
 * column.
 */
public class ColumnFilter {
    private String column;
    private int operator;
    private byte[] arguments;

    public ColumnFilter(String column, int operator, byte[] arguments) {
        setColumn(column);
        setOperator(operator);
        setArguments(arguments);
    }

    /**
     * Gets the target column that this filter applies to
     *
     * @return Label of the target column
     */
    public String getColumn() {
        return column;
    }

    /**
     * Sets the target column that this filter applies to
     */
    public void setColumn(String column) {
        this.column = column;
    }

    public int getOperator() {
        return operator;
    }

    public void setOperator(int operator) {
        this.operator = operator;
    }

    public byte[] getArguments() {
        return arguments;
    }

    public void setArguments(byte[] arguments) {
        this.arguments = arguments;
    }

    public Filter toPrimaryFilter(HaloTable table) throws IOException {
        int icol = table.findColumn(column);
        if (icol == -1) {
            throw new IOException("Column " + column + " not found in table "
                    + table.getTableProperty().getName());
        }

        ColumnProperty columnProperty = table.getTableProperty().getColumnProperty(icol);
        byte[] value = null;

        switch (operator) {
            case ColumnFilterOperator.LESS:
                value = columnProperty.getDataType().valueOf(arguments);
                return new SingleColumnValueFilter(HaloAdmin.PRIMARY_FAMILY, column.getBytes(),
                        CompareFilter.CompareOp.LESS, value);

            case ColumnFilterOperator.LESS_OR_EQUAL:
                value = columnProperty.getDataType().valueOf(arguments);
                return new SingleColumnValueFilter(HaloAdmin.PRIMARY_FAMILY, column.getBytes(),
                        CompareFilter.CompareOp.LESS_OR_EQUAL, value);

            case ColumnFilterOperator.EQUAL:
                value = columnProperty.getDataType().valueOf(arguments);
                return new SingleColumnValueFilter(HaloAdmin.PRIMARY_FAMILY, column.getBytes(),
                        CompareFilter.CompareOp.EQUAL, value);

            case ColumnFilterOperator.NOT_EQUAL:
                value = columnProperty.getDataType().valueOf(arguments);
                return new SingleColumnValueFilter(HaloAdmin.PRIMARY_FAMILY, column.getBytes(),
                        CompareFilter.CompareOp.NOT_EQUAL, value);

            case ColumnFilterOperator.GREATER:
                value = columnProperty.getDataType().valueOf(arguments);
                return new SingleColumnValueFilter(HaloAdmin.PRIMARY_FAMILY, column.getBytes(),
                        CompareFilter.CompareOp.GREATER, value);

            case ColumnFilterOperator.GREATER_OR_EQUAL:
                value = columnProperty.getDataType().valueOf(arguments);
                return new SingleColumnValueFilter(HaloAdmin.PRIMARY_FAMILY, column.getBytes(),
                        CompareFilter.CompareOp.GREATER_OR_EQUAL, value);

            case ColumnFilterOperator.BETWEEN_AND:
                try {
                    FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                    BetweenAndArguments arguments = BetweenAndArguments.valueOf(this.arguments);
                    byte[] startValue = columnProperty.getDataType().valueOf(arguments.getStartValue());
                    byte[] stopValue = columnProperty.getDataType().valueOf(arguments.getStopValue());
                    list.addFilter(new SingleColumnValueFilter(HaloAdmin.PRIMARY_FAMILY, column.getBytes(),
                            CompareFilter.CompareOp.GREATER_OR_EQUAL, startValue));
                    list.addFilter(new SingleColumnValueFilter(HaloAdmin.PRIMARY_FAMILY, column.getBytes(),
                            CompareFilter.CompareOp.LESS_OR_EQUAL, stopValue));
                    return list;
                } catch (Exception e) {
                    throw new IOException(e.getMessage());
                }

            default:
                throw new IOException("Unimplemented filter operation, id=" + operator);
        }
    }

    public RowSet applyToTable(HaloTable table) throws IOException {
        int icol = table.findColumn(column);
        if (icol == -1) {
            throw new IOException("Column " + column + " not found in table "
                    + table.getTableProperty().getName());
        }

        Scan scan = new Scan();
        ColumnProperty columnProperty = table.getTableProperty().getColumnProperty(icol);
        if (!columnProperty.isIndex()){
            /**
             * TODO:
             * (1)Add filter support for nonindexed columns
             * (2)Improve filter performance by merge filters on the same column.
             */
            //scan.setFilter(toPrimaryFilter(table));
            //return table.scanPrimary(scan);
            throw new IOException("Column " + column + " is not indexed");
        }

        boolean scanIndex = true;
        byte[] value = null;
        switch (operator) {
            case ColumnFilterOperator.LESS:
                value = columnProperty.getDataType().valueOf(arguments);
                scan.setStopRow(Bytes.concat(value, RowId.LOWER_KEY));
                break;
            case ColumnFilterOperator.LESS_OR_EQUAL:
                value = columnProperty.getDataType().valueOf(arguments);
                scan.setStopRow(Bytes.concat(value, RowId.UPPER_KEY));
                break;
            case ColumnFilterOperator.EQUAL:
                value = columnProperty.getDataType().valueOf(arguments);
                scan.setStartRow(Bytes.concat(value, RowId.LOWER_KEY));
                scan.setStopRow(Bytes.concat(value, RowId.UPPER_KEY));
                break;
            case ColumnFilterOperator.NOT_EQUAL:
                // TODO fix this
                value = columnProperty.getDataType().valueOf(arguments);
                scanIndex = false;
                scan.setFilter(new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(value)));
                break;
            case ColumnFilterOperator.GREATER_OR_EQUAL:
                value = columnProperty.getDataType().valueOf(arguments);
                scan.setStartRow(Bytes.concat(value, RowId.LOWER_KEY));
                break;
            case ColumnFilterOperator.GREATER:
                value = columnProperty.getDataType().valueOf(arguments);
                scan.setStartRow(Bytes.concat(value, RowId.UPPER_KEY));
                break;
            case ColumnFilterOperator.BETWEEN_AND:
                try {
                    /* Both startValue and stopValue are included */
                    BetweenAndArguments baa = BetweenAndArguments.valueOf(arguments);
                    byte[] startValue = columnProperty.getDataType().valueOf(baa.getStartValue());
                    byte[] stopValue = columnProperty.getDataType().valueOf(baa.getStopValue());
                    scan.setStartRow(Bytes.concat(startValue, RowId.LOWER_KEY));
                    scan.setStopRow(Bytes.concat(stopValue, RowId.UPPER_KEY));
                } catch (Exception e) {
                    throw new IOException(e.getMessage());
                }
                break;
            default:
                throw new IOException("Unimplemented filter operation, id=" + operator);
        }

        return scanIndex ? table.scanIndex(table.findColumn(column), scan)
                : table.scanPrimary(scan);
    }
}
