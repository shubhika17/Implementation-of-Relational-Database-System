package heap;
import global.GlobalConst;


/**
 * Created by Shubhika on 2/20/2017.
 */
public class Tuple implements GlobalConst{
    static final int total_size = MAX_TUPSIZE;
    byte[] data;
    int offset;
    int length;

    public Tuple(){
        this.data = new byte[total_size];
        this.offset = 0;
        this.length = total_size;
    }
    public Tuple(byte[] data, int offset, int length){
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    public Tuple(byte[] record) {
        this.data = record;
        this.length = record.length;
        this.offset = 0;
    }

    public int getLength(){
        return length;
    }
    public byte[] getTupleByteArray(){
        return data;
    }
}
