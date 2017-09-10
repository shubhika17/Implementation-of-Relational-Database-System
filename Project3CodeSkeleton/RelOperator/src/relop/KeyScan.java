package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;
import global.RID;
/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {
  //Schema schema;
  HashIndex index;
  HeapFile file;
  SearchKey key;
  boolean open;
  HashScan scan;

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
    this.schema = schema;
    this.index = index;
    this.key = key;
    this.file = file;
    scan = index.openScan(key);
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
    // /throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    if(open){
      scan.close();
      open = false;
    }
    scan = index.openScan(key);
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
      RID rid = scan.getNext();
      byte[] data = file.selectRecord(rid);
      Tuple tuple = new Tuple(schema, data);
      return tuple;
    }catch (Exception e){
      throw new IllegalStateException("no tuples left");
    }
    //throw new UnsupportedOperationException("Not implemented");
  }

} // public class KeyScan extends Iterator
