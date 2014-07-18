package halo.common;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * A tool class to handle byte arrays
 */
public final class Bytes {
    public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    /**
     * Bytes of ASCII string "true"
     */
    public static final byte[] TRUE = "true".getBytes();

    /**
     * Bytes of ASCII string "false"
     */
    public static final byte[] FALSE = "false".getBytes();

    /**
     * An empty byte array length of which is zero.
     */
    public static final byte[] NULL = new byte[0];

    /**
     * One zero byte
     */
    public static final byte[] ZERO = zeros(1);

    /**
     * "yyyy-MM-dd hh:mm:ss"
     */
    public static final DateFormat DFT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public static byte[] toBytes(int value){
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    public static byte[] toBytes(long value){
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    public static byte[] toBytes(float value){
        return ByteBuffer.allocate(4).putFloat(value).array();
    }

    public static byte[] toBytes(double value){
        return ByteBuffer.allocate(8).putDouble(value).array();
    }

    public static byte[] toBytes(boolean value){
        return value ? TRUE : FALSE;
    }

    public static byte[] toBytes(String value){
        return value.getBytes(CHARSET_UTF8);
    }

    public static byte[] toBytes(Date value){
        return toBytes(DFT.format(value));
    }

    /**
     * Concatenate byte array a1 and a2.
     * @param a1, The 1st byte array to be concatenated, may be null
     * @param a2, The 2nd byte array to be concatenated, may be null
     * @return [a1, a2]
     */
    public static byte[] concat(byte[] a1, byte[] a2){
        int l1 = a1 != null ? a1.length : 0;
        int l2 = a2 != null ? a2.length : 0;
        byte[] result = new byte[l1 + l2];
        if (l1 != 0){
            System.arraycopy(a1, 0, result, 0, l1);
        }
        if (l2 != 0){
            System.arraycopy(a2, 0, result, l1, l2);
        }
        return result;
    }

    /**
     * Concatenate byte array a1 and a2.
     * @param a1, The 1st byte array to be concatenated, may be null
     * @param a2, The 2nd byte array to be concatenated, may be null
     * @param a3, The 3rd byte array to be concatenated, may be null
     * @return [a1, a2, a3]
     */
    public static byte[] concat(byte[] a1, byte[] a2, byte[] a3){
        int l1 = a1 != null ? a1.length : 0;
        int l2 = a2 != null ? a2.length : 0;
        int l3 = a3 != null ? a3.length : 0;
        byte[] result = new byte[l1 + l2 + l3];
        if (l1 != 0){
            System.arraycopy(a1, 0, result, 0, l1);
        }
        if (l2 != 0){
            System.arraycopy(a2, 0, result, l1, l2);
        }
        if (l3 != 0){
            System.arraycopy(a3, 0, result, l1+l2, l3);
        }
        return result;
    }

    /**
     * Creates an array that is filled with specified value.
     * @param value value used to fill the byte array.
     * @param length length of the byte array to be created.
     * @return A value filled byte array.
     */
    public static byte[] array(byte value, int length){
        byte[] a = new byte[length];
        Arrays.fill(a, value);
        return a;
    }

    /**
     * Gets a ranged copy of a byte array.
     * @param source The source byte array to copy.
     * @param offset Where the copy starts from.
     * @param length Number of bytes to be copied.
     * @return A copy of bytes within the specified range of source byte array.
     */
    public static byte[] range(byte[] source, int offset, int length) {
        byte[] copy = new byte[length];
        System.arraycopy(source, offset, copy, 0, length);
        return copy;
    }

    /**
     * Creates a byte array that is filled with zero's.
     * @param length length of the byte array to be created.
     * @return A zero filled byte array.
     */
    public static byte[] zeros(int length){
        return array((byte)0, length);
    }

    public static int toInt(byte[] array){
        return ByteBuffer.wrap(array).getInt();
    }

    public static long toLong(byte[] array){
        return ByteBuffer.wrap(array).getLong();
    }

    public static float toFloat(byte[] array){
        return ByteBuffer.wrap(array).getFloat();
    }

    public static double toDouble(byte[] array){
        return ByteBuffer.wrap(array).getDouble();
    }

    public static boolean toBoolean(byte[] array){
        return Arrays.equals(array, TRUE);
    }

    public static String toString(byte[] array){
        return new String(array, CHARSET_UTF8);
    }

    public static Date toDate(byte[] array){
        try{
            return DFT.parse(toString(array));
        }
        catch (ParseException e){}
        return null;
    }

    public static int compare(byte[] left, byte[] right) {
        return org.apache.hadoop.hbase.util.Bytes.compareTo(left, right);
    }

    public static int compare(byte[] buf1, int off1, int len1, byte[] buf2, int off2, int len2) {
        return org.apache.hadoop.hbase.util.Bytes.compareTo(buf1, off1, len1, buf2, off2, len2);
    }
}
