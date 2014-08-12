package halo.client;

import halo.common.Bytes;
import halo.core.*;
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

    public ColumnScans toColumnScans(HaloTable table) throws IOException {
        int icol = table.findColumn(column);
        if (icol == -1) {
            throw new IOException("Column " + column + " not found in table "
                    + table.getTableProperty().getName());
        }

        ColumnProperty columnProperty = table.getTableProperty().getColumnProperty(icol);
        ColumnScans columnScans = new ColumnScans(icol);
        byte[] value = null;
        switch (operator) {
            case ColumnFilterOperator.LESS:
                value = columnProperty.getDataType().valueOf(arguments);
                columnScans.add(null, Bytes.concat(value, RowId.LOWER_KEY));
                break;
            case ColumnFilterOperator.LESS_OR_EQUAL:
                value = columnProperty.getDataType().valueOf(arguments);
                columnScans.add(null, Bytes.concat(value, RowId.UPPER_KEY));
                break;
            case ColumnFilterOperator.EQUAL:
                value = columnProperty.getDataType().valueOf(arguments);
                columnScans.add(Bytes.concat(value, RowId.LOWER_KEY), Bytes.concat(value, RowId.UPPER_KEY));
                break;
            case ColumnFilterOperator.NOT_EQUAL:
                value = columnProperty.getDataType().valueOf(arguments);
                columnScans.add(null, Bytes.concat(value, RowId.LOWER_KEY));
                columnScans.add(Bytes.concat(value, RowId.UPPER_KEY), null);
                break;
            case ColumnFilterOperator.GREATER_OR_EQUAL:
                value = columnProperty.getDataType().valueOf(arguments);
                columnScans.add(Bytes.concat(value, RowId.LOWER_KEY), null);
                break;
            case ColumnFilterOperator.GREATER:
                value = columnProperty.getDataType().valueOf(arguments);
                columnScans.add(Bytes.concat(value, RowId.UPPER_KEY), null);
                break;
            case ColumnFilterOperator.BETWEEN_AND:
                try {
                    /* Both startValue and stopValue are included */
                    BetweenAndArguments baa = BetweenAndArguments.valueOf(arguments);
                    byte[] startValue = columnProperty.getDataType().valueOf(baa.getStartValue());
                    byte[] stopValue = columnProperty.getDataType().valueOf(baa.getStopValue());
                    columnScans.add(Bytes.concat(startValue, RowId.LOWER_KEY), Bytes.concat(stopValue, RowId.UPPER_KEY));
                } catch (Exception e) {
                    throw new IOException(e.getMessage());
                }
                break;
            default:
                throw new IOException("Unimplemented filter operation, id=" + operator);
        }

        return columnScans;
    }
}
