package halo.core;

import halo.client.InvalidArgument;
import halo.common.Bytes;

/**
 * Descriptor for a specific data type.
 */
public class DataType {
    public static final int TYPEID_INT32 = 1;
    public static final int TYPEID_INT64 = 2;
    public static final int TYPEID_FLOAT = 3;
    public static final int TYPEID_DOUBLE = 4;
    public static final int TYPEID_DATETIME = 5;
    public static final int TYPEID_STRING = 6;

    /* Predefined fixed length types */
    public static final DataType INT32 = new DataType(TYPEID_INT32, 4, "INT32");
    public static final DataType INT64 = new DataType(TYPEID_INT64, 8, "INT64");
    public static final DataType FLOAT = new DataType(TYPEID_FLOAT, 4, "FLOAT");
    public static final DataType DOUBLE = new DataType(TYPEID_DOUBLE, 8, "DOUBLE");
    public static final DataType DATETIME = new DataType(TYPEID_DATETIME, 19, "DATETIME");

    private int typeId;
    private int maxLength;
    private String faceName;

    public DataType(int typeId, int maxLength, String faceName) {
        this.typeId = typeId;
        this.maxLength = maxLength;
        this.faceName = faceName;
    }

    /* Creates a descriptor for varchar(size) */
    public static DataType varchar(int size) {
        return new DataType(TYPEID_STRING, size, "VARCHAR(" + size + ")");
    }

    @Override
    public String toString() {
        return getFaceName();
    }

    public String getFaceName() {
        return faceName;
    }

    public int getTypeId() {
        return typeId;
    }

    public int getMaxLength() {
        return maxLength;
    }

    /**
     * Serialize a string represented value of this DataType
     * @param stringBytes bytes of the string represented value.
     * @return Serialized representation of the value.
     */
    public byte[] valueOf(byte[] stringBytes) {
        switch (typeId){
            case DataType.TYPEID_INT32:
                return Bytes.toBytes(Integer.valueOf(Bytes.toString(stringBytes)));
            case DataType.TYPEID_INT64:
                return Bytes.toBytes(Long.valueOf(Bytes.toString(stringBytes)));
            case DataType.TYPEID_FLOAT:
                return Bytes.toBytes(Float.valueOf(Bytes.toString(stringBytes)));
            case DataType.TYPEID_DOUBLE:
                return Bytes.toBytes(Double.valueOf(Bytes.toString(stringBytes)));
            default:
                return stringBytes;
        }
    }

    public static DataType fromFaceName(String faceName) throws InvalidArgument {
        faceName = faceName.toUpperCase();
        if (faceName.equals(INT32.faceName)) {
            return INT32;
        }
        if (faceName.equals(INT64.faceName)) {
            return INT64;
        }
        if (faceName.equals(FLOAT.faceName)) {
            return FLOAT;
        }
        if (faceName.equals(DOUBLE.faceName)) {
            return DOUBLE;
        }
        if (faceName.equals(DATETIME.faceName)) {
            return DATETIME;
        }
        if (faceName.startsWith("VARCHAR(") && faceName.endsWith(")")) {
            int maxLength = Integer.valueOf(faceName.substring(8, faceName.length()-1));
            return new DataType(TYPEID_STRING, maxLength, faceName);
        }

        throw new InvalidArgument("Bad DataType face name: " + faceName);
    }
}
