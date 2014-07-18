package halo.core;

import java.util.Date;

/**
 * Created by riz on 14-7-1.
 */
public class TableProperty {
    private String name;
    private ColumnProperty[] columnProperties;
    private long nextRowId;
    private String owner;
    private Date createDate;

    public TableProperty(String tableName, ColumnProperty[] cols){
        setName(tableName);
        setNextRowId(1);
        setColumnProperties(cols);
        setCreateDate(new Date());
    }

    public String getName() {
        return name;
    }

    public TableProperty setName(String name) {
        this.name = name;
        return this;
    }

    public ColumnProperty[] getColumnProperties() {
        return columnProperties;
    }

    public TableProperty setColumnProperties(ColumnProperty[] columnProperties) {
        this.columnProperties = columnProperties;
        return this;
    }

    public String getOwner() {
        return owner;
    }

    public TableProperty setOwner(String owner) {
        this.owner = owner;
        return this;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public TableProperty setCreateDate(Date createDate) {
        this.createDate = createDate;
        return this;
    }

    public long getNextRowId(){
        return nextRowId++;
    }

    public TableProperty setNextRowId(long rowid){
        nextRowId = rowid;
        return this;
    }

    public int getNumberOfColumns(){
        return getColumnProperties().length;
    }

    public ColumnProperty getColumnProperty(int column){
        return getColumnProperties()[column];
    }

    public int findColumn(String columnLabel){
        for (int i = 0; i < getColumnProperties().length; ++i){
            if (getColumnProperties()[i].getLabel().equals(columnLabel)){
                return i;
            }
        }
        return -1;
    }

    public String getPrimaryTableName(){
        return getPrimaryTableName(getName());
    }

    public String getIndexTableName(int column){
        return getIndexTableName(getName(), getColumnProperties()[column].getLabel());
    }

    public static String getPrimaryTableName(String tableName){
        return tableName + ".primary";
    }

    public static String getIndexTableName(String tableName, String columnLabel){
        return tableName + "." + columnLabel + ".index";
    }

}
