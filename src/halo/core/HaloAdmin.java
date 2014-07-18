package halo.core;

import halo.common.Bytes;
import halo.common.ColumnSpecifierHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by riz on 14-7-1.
 */
public class HaloAdmin {
    static class FieldSpecifierFormatter implements ColumnSpecifierHelper.Formatter{
        private String fieldLabel;
        public FieldSpecifierFormatter(String fieldLabel){
            this.fieldLabel = fieldLabel;
        }

        @Override
        public byte[] format(int column) {
            return (column + ":" + fieldLabel).getBytes();
        }
    }

    static ColumnSpecifierHelper labelspec =
            new ColumnSpecifierHelper(new FieldSpecifierFormatter("label"), 32);
    static ColumnSpecifierHelper dtypespec =
            new ColumnSpecifierHelper(new FieldSpecifierFormatter("dtype"), 32);
    static ColumnSpecifierHelper indexspec =
            new ColumnSpecifierHelper(new FieldSpecifierFormatter("index"), 32);

    public static final String METADATA_TABLE = "halo.metadata";
    public static final byte[] PRIMARY_FAMILY = "d".getBytes();
    public static final byte[] INDEX_FAMILY = "i".getBytes();
    public static final byte[] METADATA_FAMILY = "meta".getBytes();
    public static final byte[] METADATA_DATE = "date".getBytes();
    public static final byte[] METADATA_OWNER = "owner".getBytes();
    public static final byte[] METADATA_COLUMNS = "columns".getBytes();

    Configuration conf;
    HBaseAdmin hBaseAdmin;
    HTable metadataTable;

    public HaloAdmin(Configuration conf) throws IOException{
        this.conf = conf;
        hBaseAdmin = new HBaseAdmin(conf);

        if (!hBaseAdmin.tableExists(METADATA_TABLE)){
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(METADATA_TABLE));
            desc.addFamily(new HColumnDescriptor(METADATA_FAMILY).setMaxVersions(1));
            hBaseAdmin.createTable(desc);
        }
        metadataTable = new HTable(conf, METADATA_TABLE);
    }

    public boolean tableExists(String tableName) throws IOException{
        Result result = metadataTable.get(new Get(tableName.getBytes()));
        return null != getMetadata(result, METADATA_OWNER);
    }

    public void createTable(TableProperty tableProperty) throws IOException{
        if (tableExists(tableProperty.getName())){
            throw new IOException("Table " + tableProperty.getName() + " already exists.");
        }

        /* Create index tables */
        int columns = tableProperty.getNumberOfColumns();
        for (int i = 0; i < columns; ++i){
            if (tableProperty.getColumnProperty(i).isIndex()){
                HTableDescriptor idesc = new HTableDescriptor(
                        TableName.valueOf(tableProperty.getIndexTableName(i)));
                idesc.addFamily(new HColumnDescriptor(INDEX_FAMILY).setMaxVersions(1));
                hBaseAdmin.createTable(idesc);
            }
        }

        /* Create primary table */
        HTableDescriptor desc = new HTableDescriptor(
                TableName.valueOf(tableProperty.getPrimaryTableName()));
        desc.addFamily(new HColumnDescriptor(PRIMARY_FAMILY).setMaxVersions(1));
        hBaseAdmin.createTable(desc);

        /* Register this table */
        storeTableProperty(tableProperty);
    }

    public HaloTable openTable(String tableName) throws IOException {
        return new HaloTable(conf, readTableProperty(tableName));
    }

    public void dropTable(String tableName) throws IOException{
        TableProperty tableProperty = readTableProperty(tableName);
        TableName primaryTableName = TableName.valueOf(tableProperty.getPrimaryTableName());
        hBaseAdmin.disableTable(primaryTableName);
        hBaseAdmin.deleteTable(primaryTableName);
        int columns = tableProperty.getNumberOfColumns();
        for (int i = 0; i < columns; ++i){
            if (tableProperty.getColumnProperty(i).isIndex()){
                TableName indexTableName = TableName.valueOf(tableProperty.getIndexTableName(i));
                hBaseAdmin.disableTable(indexTableName);
                hBaseAdmin.deleteTable(indexTableName);
            }
        }

        /* Unregister this table */
        unregisterTable(tableProperty);
    }

    /**
     * List all tables
     * @return A list of table names
     * @throws IOException
     */
    public List<String> listTables() throws IOException{
        ArrayList<String> tables = new ArrayList<String>();
        ResultScanner scanner = metadataTable.getScanner(new Scan());
        for (Result r = scanner.next(); r != null; r = scanner.next()){
            tables.add(Bytes.toString(r.getRow()));
        }
        return tables;
    }

    public void createIndex(String tableName, String columnLabel){
        /* TODO */
    }

    static byte[] getMetadata(Result result, byte[] col){
        return result.getValue(METADATA_FAMILY, col);
    }

    /**
     * Reads table properties from metadata table
     * @param tableName
     * @return
     * @throws IOException
     */
    public TableProperty readTableProperty(String tableName) throws IOException{
        Result result = metadataTable.get(new Get(tableName.getBytes()));
        if (result.isEmpty()){
            throw new IOException("Table " + tableName + " doesn't exist");
        }

        int columns = Bytes.toInt(getMetadata(result, METADATA_COLUMNS));
        ColumnProperty[] columnProperties = new ColumnProperty[columns];
        try {
            for (int i = 0; i < columns; ++i){
                columnProperties[i] = new ColumnProperty(
                        Bytes.toString(getMetadata(result, labelspec.get(i))),
                        DataType.fromFaceName(new String(getMetadata(result, dtypespec.get(i)))),
                        Bytes.toBoolean(getMetadata(result, indexspec.get(i)))
                );
            }
        }
        catch (Exception e){
            throw new IOException("Corrupted metadata table: " + e.getMessage());
        }

        TableProperty prop = new TableProperty(tableName, columnProperties);
        prop.setOwner(Bytes.toString(getMetadata(result, METADATA_OWNER)));
        prop.setCreateDate(Bytes.toDate(getMetadata(result, METADATA_DATE)));
        return prop;
    }

    /**
     * Stores table properties to metadata table
     * @param tableProperty
     * @throws IOException
     */
    private void storeTableProperty(TableProperty tableProperty) throws IOException{
        Put put = new Put(tableProperty.getName().getBytes());
        int columns = tableProperty.getNumberOfColumns();
        put.add(METADATA_FAMILY, METADATA_DATE, Bytes.toBytes(tableProperty.getCreateDate()));
        put.add(METADATA_FAMILY, METADATA_OWNER, Bytes.toBytes(tableProperty.getOwner()));
        put.add(METADATA_FAMILY, METADATA_COLUMNS, Bytes.toBytes(columns));

        for (int i = 0; i < columns; ++i){
            ColumnProperty prop = tableProperty.getColumnProperty(i);
            put.add(METADATA_FAMILY, labelspec.get(i), Bytes.toBytes(prop.getLabel()));
            put.add(METADATA_FAMILY, dtypespec.get(i), prop.getDataType().getFaceName().getBytes());
            put.add(METADATA_FAMILY, indexspec.get(i), Bytes.toBytes(prop.isIndex()));
        }
        metadataTable.put(put);
    }

    private void unregisterTable(TableProperty tableProperty) throws IOException {
        metadataTable.delete(new Delete(tableProperty.getName().getBytes()));
    }
}
