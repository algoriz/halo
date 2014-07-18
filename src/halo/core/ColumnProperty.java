package halo.core;


/**
 * Created by riz on 14-7-1.
 */
public class ColumnProperty {
    private String label;
    private DataType dataType;
    private boolean index;

    public ColumnProperty(String label, DataType dataType, boolean index) {
        setLabel(label);
        setDataType(dataType);
        setIndex(index);
    }

    public DataType getDataType() {
        return dataType;
    }

    public int getMaxLength() {
        return dataType.getMaxLength();
    }

    public String getLabel() {
        return label;
    }

    public boolean isIndex() {
        return index;
    }

    public ColumnProperty setLabel(String label) {
        this.label = label;
        return this;
    }

    public ColumnProperty setDataType(DataType dataType) {
        this.dataType = dataType;
        return this;
    }

    public ColumnProperty setIndex(boolean index) {
        this.index = index;
        return this;
    }
}
