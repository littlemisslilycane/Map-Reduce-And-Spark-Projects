import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;

public class TripleTuple implements WritableComparable<TripleTuple> {

    private int x;
    private int y;
    private int z;


    public TripleTuple() {
    }

    public TripleTuple(int x, int y, int z) {
        this.x = x;
        this.y = y;
        this.z = z;

    }


    public int getX() {
        return this.x;
    }

    public int getY() {
        return this.y;
    }

    public int getZ() {
        return this.z;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(x);
        out.writeInt(y);
        out.writeInt(z);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        x = in.readInt();
        y = in.readInt();
        z = in.readInt();
    }

    @Override
    public int hashCode() {
        return x * y * z;
    }

    @Override
    public boolean equals(Object o) {
        return compareTo((TripleTuple) o) == 0;
    }

    @Override
    public String toString() {
        return x + "," + y + "," + z;
    }

    @Override
    public int compareTo(TripleTuple p) {

        if ((this.x == p.getY() && this.y == p.getZ() && this.z == p.getX()) || this.x == p.getZ() && this.y == p.getX() && this.z == p.getY()) {
            return 0;
        } else {
            return 1;
        }
    }


}
