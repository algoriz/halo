package halo.client;

import halo.core.ColumnScans;
import halo.core.HaloTable;
import halo.core.RowSet;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

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
    private ColumnScans columnScans;

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

    private void buildScans(HaloTable table) throws IOException {
        if (isLeafNode()){
            columnScans = columnFilter.toColumnScans(table);
        } else {
            leftChild.buildScans(table);
            rightChild.buildScans(table);

            if (leftChild.columnScans != null && rightChild.columnScans != null &&
                    leftChild.columnScans.isMergable(rightChild.columnScans)){
                columnScans = connector == OR ?
                        leftChild.columnScans.unionMerge(rightChild.columnScans) :
                        leftChild.columnScans.intersectMerge(rightChild.columnScans);
            } else {
                columnScans = null;
            }
        }
    }

    private RowSet applyScans(HaloTable table) throws IOException {
        if (isEmpty()){
            Scan scan = new Scan();
            /**
             *  Actually we donot want any KV's, we need only the RowKey.
             *  Using a FirstKeyOnlyFilter here will speed up the scan process.
             */
            scan.setFilter(new FirstKeyOnlyFilter());
            return table.scanPrimary(scan);
        }

        if (columnScans != null){
            return table.scan(columnScans);
        }

        RowSet left = leftChild.applyToTable(table);
        RowSet right = rightChild.applyToTable(table);
        return connector == OR ? left.union(right) : left.intersect(right);
    }

    public RowSet applyToTable(HaloTable table) throws IOException {
        buildScans(table);
        return applyScans(table);
    }
}

