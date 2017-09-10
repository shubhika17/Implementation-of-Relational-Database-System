package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;
/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

  /**
   * Constructs a file scan, given the schema and heap file.
   */
  //Schema schema;
  HeapScan scan;
  HeapFile file;
  RID rid;
  boolean open;

  public FileScan(Schema schema, HeapFile file) {
    this.schema = schema;
    this.file = file;
    this.scan = file.openScan();
    this.open = true;
    this.rid = new RID();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    indent(depth);
    System.out.println("Explaination: "+file.toString());
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    if(open){
      scan.close();
      open = false;
    }
    scan = file.openScan();
    open = true;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return open;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    if(open){
      scan.close();
      open = false;
    }
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    if(open){
      return scan.hasNext();
    }
    return false;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if(!open)
      throw new IllegalStateException("not open!!");
    try {
      byte[] data =scan.getNext(rid);
      Tuple tuple =new Tuple(schema,data);
      return tuple;
    }catch (Exception e){
      throw new IllegalStateException("no tuples left");
    }
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
    RID temp=new RID();
    temp.copyRID(this.rid);
    return temp;
    //throw new UnsupportedOperationException("Not implemented");
  }

} // public class FileScan extends Iterator
