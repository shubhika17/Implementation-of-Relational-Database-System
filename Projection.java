package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {
  Iterator iter;
  Integer[] fields;


  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
    this.iter = iter;
    this.fields = fields;
    this.schema = new Schema(fields.length);
    for (int i = 0; i < fields.length; i++)
      schema.initField(i, iter.schema, fields[i]);

    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    indent(depth);
    iter.explain(depth);
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    this.iter.restart();

    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return this.iter.isOpen();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    this.iter.close();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return iter.hasNext() && isOpen();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if (hasNext()) {
      Tuple tuple = iter.getNext();
      //Tuple tuple = new Tuple(schema);
      //for (int i = 0; i < fields.length; i++) {
        //tuple.setField(i, temp.getField(fields[i]));
      //}
      System.out.println(tuple);
      return tuple;

    }
    throw new IllegalStateException("no tuples left");
    //throw new UnsupportedOperationException("Not implemented");
  }

} // public class Projection extends Iterator
