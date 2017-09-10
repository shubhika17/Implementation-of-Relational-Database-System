package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

  Iterator iter;
  Integer[] fields;
  Tuple tuple;

  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
    this.iter = iter;
    this.fields = fields;
    this.schema = new Schema(fields.length);
    for (int i = 0; i < fields.length; i++)
      schema.initField(i, iter.schema, fields[i]);
    tuple = null;
    // throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    iter.explain(depth + 1);
    // throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    iter.restart();
    tuple = null;
    // throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return iter.isOpen();
    // throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    iter.close();
    tuple = null;
    // throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {

    if (iter.hasNext() && isOpen()) {
      Tuple temp = iter.getNext();
      tuple = new Tuple(schema);
      for (int i = 0; i < fields.length; i++) {
        tuple.setField(i, temp.getField(fields[i]));

      }
      return true;
    }

    tuple = null;
    return false;

  }

  /**
   * Gets the next tuple in the iteration.
   *
   * @throws IllegalStateException
   *             if no more tuples
   */
  public Tuple getNext() {
    if (tuple == null)
      throw new IllegalStateException("no tuple error");
    return tuple;
    // throw new UnsupportedOperationException("Not implemented");
  }

} // public class Projection extends Iterator