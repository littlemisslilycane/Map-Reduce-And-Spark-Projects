import java.io.*;

import org.apache.hadoop.io.*;

public class MyWritable implements WritableComparable<MyWritable> {

    private int x;
    private int y;
    private String type;


    public MyWritable() {
    }

    public MyWritable(int x, int y, String type) {
        this.x = x;
        this.y = y;
        this.type = type;

    }
    public MyWritable(int x, int y) {
        this.x = x;
        this.y = y;
        this.type = "DEFAULT";

    }

    public int getX() {
        return this.x;
    }

    public int getY() {
        return this.y;
    }
    public String getType() {
        return this.type;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(x);
        out.writeInt(y);
        out.writeBytes(type);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        x = in.readInt();
        y = in.readInt();
        type = in.readLine();
    }

    @Override
    public int hashCode() {
        return x * 163 + y;
    }

    @Override
    public boolean equals(Object o) {
        return compareTo((MyWritable) o) == 0;
    }

    @Override
    public    String toString() {
        return x + "\t" + y + "\t" + type;
    }

    @Override
    public  int compareTo(MyWritable p) {
        int cmp = compare(x,p.x );
        return cmp;
    }

    /*** Convenience method for comparing two ints.*/
    public static int compare(int a, int b) {
        return (a < b ? -1 : (a == b ? 0 : 1));
    }

}
