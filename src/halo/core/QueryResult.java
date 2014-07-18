package halo.core;


import java.util.LinkedList;

/**
 * Created by riz on 14-6-30.
 */
public class QueryResult{
    public QueryResult(int columns){
        this.columns = columns;
        this.rows = new LinkedList<byte[][]>();
    }

    public int getNumberOfColumns(){
        return columns;
    }

    public int getNumberOfRows(){
        return rows.size();
    }

    public byte[][] getRow(int row){
        return rows.get(row);
    }

    public void add(byte[][] row){
        rows.add(row);
    }

    int columns;
    LinkedList<byte[][]> rows;
}
