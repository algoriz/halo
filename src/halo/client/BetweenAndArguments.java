package halo.client;

import halo.common.Bytes;

import java.util.Arrays;

/**
 * Arguments for BETWEEN-AND operator in a ColumnFilter
 */
public class BetweenAndArguments {
    byte[] startValue;
    byte[] stopValue;

    public byte[] getStartValue() {
        return startValue;
    }

    public byte[] getStopValue() {
        return stopValue;
    }

    public void setStartValue(byte[] startValue) {
        this.startValue = startValue;
    }

    public void setStopValue(byte[] stopValue) {
        this.stopValue = stopValue;
    }

    /**
     * Deserializes a BetweenAndArguments object from the byte array.
     * @param value byte array results from BetweenAndArguments.toBytes
     * @return The BetweenAndArguments object parsed from the byte array.
     * @throws InvalidArgument
     */
    public static BetweenAndArguments valueOf(byte[] value) throws InvalidArgument{
        int len = Bytes.toInt(Bytes.range(value, 0, 4));
        if (value.length < len){
            throw new InvalidArgument("Not a valid BetweenAndArguments");
        }
        BetweenAndArguments arguments = new BetweenAndArguments();
        arguments.setStartValue(
                len != 0 ? Bytes.range(value, 4, len) : Bytes.NULL);
        arguments.setStopValue(
                len != value.length ? Arrays.copyOfRange(value, 4+len, value.length) : Bytes.NULL);
        return arguments;
    }

    /**
     * Serializes a BetweenAndArguments
     */
    public byte[] toBytes() {
        return Bytes.concat(Bytes.toBytes(startValue.length), startValue, stopValue);
    }
}
