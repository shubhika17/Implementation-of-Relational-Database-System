package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {
  //Schema schema;
  HashIndex index;
  HeapFile file;
  boolean open;
  BucketScan scan;
  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
    this.schema = schema;
    this.index = index;
    this.file = file;
    scan = this.index.openScan();
    open = true;
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
    scan = index.openScan();
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
    if(!open || !hasNext())
      throw new IllegalStateException("not open!!");
    try {
      RID rid = scan.getNext();
      byte[] data = file.selectRecord(rid);
      Tuple tuple = new Tuple(schema, data);
      return tuple;
    }catch (Exception e){
      throw new IllegalStateException("no tuples left");
    }
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
    return scan.getLastKey();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
    return scan.getNextHash();
    //throw new UnsupportedOperationException("Not implemented");
  }

} // public class IndexScan extends Iterator
