package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {
  Iterator iter;
  Predicate[] preds;
  Tuple tuple;
  boolean nextTuple;
  int badtuple = 0;
  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator iter, Predicate... preds) {
    this.schema = iter.schema;
    this.iter = iter;
    this.preds = preds;
    this.tuple = null;
    nextTuple = false;
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
    tuple = null;
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
    if(!isOpen())
      return false;
    while(iter.hasNext()){;
      Tuple temp=iter.getNext();
      for(int i = 0; i < preds.length; i++){
        //if(!pred.evaluate(temp)){
          //System.out.println("bad tuple");
          //badtuple++;
          //   correct=false;
          //   break;
        //}
        if(preds[i].evaluate(temp)) {
          this.tuple=temp;
          nextTuple = true;
          return true;
        }
      }
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
    if(nextTuple){
      //System.out.println("Selection here");
      nextTuple = false;
      return tuple;
    }
    if(hasNext()) {
      nextTuple = false;
      //System.out.println("Selection here");
      return tuple;
    }
    //System.out.println(badtuple);
    System.out.println("EXCEPTION IN SELECTION");
    throw new IllegalStateException("no tuples left");
  }

} // public class Selection extends Iterator
