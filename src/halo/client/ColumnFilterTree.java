package halo.client;

import halo.core.HaloTable;
import halo.core.RowSet;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;

import java.io.IOException;

/**
 * Created by riz on 14-7-2.
 */

public class ColumnFilterTree {
    public static final int OR = 0;
    public static final int AND = 1;

    private ColumnFilterTree leftChild;
    private int connector;
    private ColumnFilterTree rightChild;
    private ColumnFilter columnFilter;

    /**
     * Constructs an empty node
     */
    public ColumnFilterTree(){}

    /**
     * Constructs a non-leaf node
     * @param leftChild
     * @param connector
     * @param rightChild
     */
    public ColumnFilterTree(
            ColumnFilterTree leftChild,
            int connector,
            ColumnFilterTree rightChild){
        setLeftChild(leftChild);
        setConnector(connector);
        setRightChild(rightChild);
        setColumnFilter(null);
    }

    /**
     * Constructs a leaf node
     * @param columnFilter
     */
    public ColumnFilterTree(ColumnFilter columnFilter){
        setLeftChild(null);
        setRightChild(null);
        setColumnFilter(columnFilter);
    }

    public boolean isEmpty() {
        return columnFilter == null && leftChild == null && rightChild == null;
    }

    public boolean isLeafNode() {
        return columnFilter != null;
    }

    public ColumnFilterTree getLeftChild() {
        return leftChild;
    }

    public void setLeftChild(ColumnFilterTree leftChild) {
        this.leftChild = leftChild;
    }

    public ColumnFilterTree getRightChild() {
        return rightChild;
    }

    public void setRightChild(ColumnFilterTree rightChild) {
        this.rightChild = rightChild;
    }

    public int getConnector() {
        return connector;
    }

    public void setConnector(int connector) {
        this.connector = connector;
    }

    public ColumnFilter getColumnFilter() {
        return columnFilter;
    }

    public void setColumnFilter(ColumnFilter columnFilter) {
        this.columnFilter = columnFilter;
    }

    public RowSet applyToTable(HaloTable table) throws IOException {
        if (isEmpty()){
            Scan scan = new Scan();
            /**
             *  Actually we donot want any KV's, we need only the RowKey.
             *  Using a FirstKeyOnlyFilter here will speed up the scan process.
             */
            scan.setFilter(new FirstKeyOnlyFilter());
            return table.scanPrimary(scan);
        }

        if (isLeafNode()){
            return columnFilter.applyToTable(table);
        }

        /**
         *  An optimization for filters like "X > a and X < b"
         */
        if (leftChild.isLeafNode() && rightChild.isLeafNode()
                && connector == AND
                && leftChild.getColumnFilter().getColumn().equals(rightChild.getColumnFilter().getColumn())){

        }

        RowSet left = leftChild.applyToTable(table);
        RowSet right = rightChild.applyToTable(table);
        return connector == OR ? left.union(right) : left.intersect(right);
    }
}

