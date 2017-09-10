package relop;

import java.util.*;

import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import heap.HeapFile;
import heap.HeapScan;

public class SortMergeJoin extends Iterator {

    private Iterator outer;
    private Iterator inner;

    private HeapFile combineData;
    private HeapScan scan;
    private Predicate[] preds;
    final int schemai;
    final int schemaj;

    public SortMergeJoin(Iterator left, Iterator right, int x, int y) {
        ArrayList<HeapFile> leftTuples;
        ArrayList<HeapFile> rightTuples;
        this.outer = left;
        this.inner = right;
        this.schema = Schema.join(left.schema, right.schema);
        leftTuples = new ArrayList<>();
        rightTuples = new ArrayList<>();
        this.preds = new Predicate[] { new Predicate(AttrOperator.EQ, AttrType.FIELDNO, x, AttrType.FIELDNO, y) };
        int size = 0;
        Tuple tuple = null;
        if(left.hasNext()) {
            tuple = left.getNext();
        }

        this.schemai = x;
        this.schemaj = y;
        int i = 0;
        int j = 0;
        while(tuple != null){
            if(size - tuple.getData().length < 0){
                size = 0;
            }
            if(size == 0){
                HeapFile name  = new HeapFile(null);
                leftTuples.add(name);
                i = 0;
                size = GlobalConst.PAGE_SIZE;
                if(leftTuples.size() > 1){
                    j++;
                }
            }

            HeapFile temp = leftTuples.get(j);
            temp.insertRecord(tuple.getData());

            //System.out.println(size);
            i++;
            if(left.hasNext()) {
                tuple = left.getNext();
                size = size - tuple.getData().length;
            }else{
                tuple = null;
            }
            if(size  < 0){
                size = 0;
            }
        }
        //System.out.println(j);
        tuple = null;
        if(right.hasNext()) {
            tuple = right.getNext();
        }
        i = 0;
        j = 0;
        size = 0;

        while(tuple != null){
            if(size - tuple.getData().length < 0){
                size = 0;
            }
            if(size == 0){
                HeapFile name  = new HeapFile(null);
                rightTuples.add(name);
                i = 0;
                size = GlobalConst.PAGE_SIZE;
                if(rightTuples.size() > 1){
                    j++;
                }
            }

            HeapFile temp = rightTuples.get(j);
            temp.insertRecord(tuple.getData());

            //System.out.println(size);
            i++;
            if(right.hasNext()) {
                tuple = right.getNext();
                size = size - tuple.getData().length;
            }else{
                tuple = null;
            }
            if(size  < 0){
                size = 0;
            }
        }

        //System.out.println(j);
        int k = 0;
        while(leftTuples.size() > k){
            leftTuples = sort(leftTuples, outer.schema, schemai);
            k++;
        }
        k = 0;
        while(rightTuples.size() > k){
            rightTuples = sort(rightTuples, inner.schema, schemaj);
            k++;
        }

        while(leftTuples.size() > 1){
            leftTuples = combine(leftTuples, outer.schema, schemai);
        }
        while(rightTuples.size() > 1){
            rightTuples = combine(rightTuples, inner.schema, schemaj);
        }

        combineData = join(leftTuples.get(0), rightTuples.get(0));
        scan = combineData.openScan();
    }

    public ArrayList<HeapFile> sort(ArrayList<HeapFile> left, Schema schema, int schemai){
        HeapScan scan1 = left.get(0).openScan();
        AbstractList<Tuple> leftTuples = new ArrayList<>();
        Tuple tuple = null;
        if(scan1.hasNext()){
            RID rid = new RID();
            byte[] data = scan1.getNext(rid);
            tuple = new Tuple(schema, data);
        }
        while(tuple != null){
            leftTuples.add(tuple);
            if(scan1.hasNext()){
                RID rid = new RID();
                byte[] data = scan1.getNext(rid);
                tuple = new Tuple(schema, data);
            }else{
                tuple = null;
            }
        }
        switch (schema.fieldType(schemai)){
            case AttrType.INTEGER:

                for (int i = 0; i < leftTuples.size(); i++){
                    for(int j = i + 1; j < leftTuples.size(); j++){
                        leftTuples.get(i).getIntFld(schemai);
                        if(leftTuples.get(i).getIntFld(schemai) > leftTuples.get(j).getIntFld(schemai)){
                            Tuple temp = leftTuples.get(i);
                            leftTuples.set(i,leftTuples.get(j));
                            leftTuples.set(j,  temp);
                        }
                    }
                }
                break;
            case AttrType.STRING:
                for (int i = 0; i < leftTuples.size(); i++){
                    for(int j = i; j < leftTuples.size(); j++){
                        if(leftTuples.get(i).getStringFld(schemai).compareTo(leftTuples.get(j).getStringFld(schemai)) > 0){
                            Tuple temp = leftTuples.get(i);
                            leftTuples.set(i,leftTuples.get(j));
                            leftTuples.set(j,  temp);
                        }
                    }
                }
                break;
            case AttrType.FLOAT:
                for (int i = 0; i < leftTuples.size(); i++){
                    for(int j = i; j < leftTuples.size(); j++){
                        if(leftTuples.get(i).getIntFld(schemai) > leftTuples.get(j).getIntFld(schemai)){
                            Tuple temp = leftTuples.get(i);
                            leftTuples.set(i,leftTuples.get(j));
                            leftTuples.set(j,  temp);
                        }
                    }
                }
                break;

        }
        scan1.close();
        left.get(0).deleteFile();
        left.remove(0);
        HeapFile name = new HeapFile(null);
        while (!leftTuples.isEmpty()){
            Tuple tuple1 = leftTuples.remove(0);
            name.insertRecord(tuple1.getData());
        }
        left.add(name);
        return left;
    }

    public ArrayList<HeapFile> combine(ArrayList<HeapFile> left,Schema schema, int schemaj){
        HeapScan scan1 = left.get(0).openScan();
        HeapScan scan2 = left.get(1).openScan();
        HeapFile combine = new HeapFile(null);
        int count = 0;
        RID rid = new RID();
        RID rid2 = new RID();
        byte[] data = null;
        byte [] data2 = null;
        Tuple tuple = null;
        Tuple tuple2 = null;
        int i = 0;
        switch (schema.fieldType(schemaj)){
            case AttrType.INTEGER:
                i++;
                //System.out.println("here");
                //System.out.println("k: " + k);
                //System.out.println("l: " + l);
                //System.out.println("size: " + leftTuples.size());
                //System.out.println("size: " + rightTuples.size());
                if(scan1.hasNext()) {
                    data = scan1.getNext(rid);
                    tuple = new Tuple(schema, data);
                }

                if(scan2.hasNext()) {
                    data2 = scan2.getNext(rid2);
                    tuple2 = new Tuple(schema, data2);
                }


                while(tuple!= null || tuple2 != null) {
                    if(tuple == null){
                        //System.out.println("tuple"+tuple2.getIntFld(schemaj));
                        combine.insertRecord(tuple2.getData());
                        count++;
                        if(scan2.hasNext()){
                            data2 = scan2.getNext(rid2);
                            tuple2 = new Tuple(schema, data2);
                        }else {
                            tuple2 = null;
                        }
                        continue;
                    }
                    if(tuple2 == null){
                        //System.out.println("weird"+tuple.getIntFld(schemaj));
                        combine.insertRecord(tuple.getData());
                        count++;
                        if(scan1.hasNext()){
                            data = scan1.getNext(rid);
                            tuple = new Tuple(schema, data);
                        }else {
                            tuple = null;
                        }
                        continue;
                    }
                    if (tuple.getIntFld(schemaj) > tuple2.getIntFld(schemaj)) {
                        //System.out.println(tuple2.getIntFld(schemaj));
                        combine.insertRecord(tuple2.getData());
                        count++;
                        if(scan2.hasNext()){
                            data2 = scan2.getNext(rid2);
                            tuple2 = new Tuple(schema, data2);
                        }else {
                            tuple2 = null;
                        }
                    }else{
                        //System.out.println(tuple.getIntFld(schemaj));
                        combine.insertRecord(tuple.getData());
                        count++;
                        if(scan1.hasNext()){
                            data = scan1.getNext(rid);
                            tuple = new Tuple(schema, data);
                        }else {
                            tuple = null;
                        }
                    }
                }
                //System.out.println("total count"+ count);
                break;
            case AttrType.STRING:
                //System.out.println("here string");
                //System.out.println("k: " + k);
                //System.out.println("l: " + l);
                //System.out.println("size: " + leftTuples.size());
                //System.out.println("size: " + rightTuples.size());
                rid = new RID();
                rid2 = new RID();
                data = null;
                if(scan1.hasNext()) {
                    data = scan1.getNext(rid);
                    tuple = new Tuple(schema, data);
                }
                data2 = null;
                if(scan2.hasNext()) {
                    data2 = scan2.getNext(rid2);
                    tuple2 = new Tuple(schema, data2);
                }
                while(tuple!= null || tuple2 != null) {
                    if(tuple == null){
                        combine.insertRecord(tuple2.getData());
                        if(scan2.hasNext()){
                            data2 = scan2.getNext(rid2);
                            tuple2 = new Tuple(schema, data2);
                        }else {
                            tuple2 = null;
                        }
                        continue;
                    }
                    if(tuple2 == null){
                        combine.insertRecord(tuple.getData());
                        if(scan1.hasNext()){
                            data = scan1.getNext(rid);
                            tuple = new Tuple(schema, data);
                        }else {
                            tuple = null;
                        }
                        continue;
                    }
                    if (tuple.getStringFld(schemaj).compareTo(tuple2.getStringFld(schemaj)) < 0) {
                        combine.insertRecord(tuple.getData());
                        if(scan1.hasNext()){
                            data = scan1.getNext(rid);
                            tuple = new Tuple(schema, data);
                        }else {
                            tuple = null;
                        }
                    }else{
                        combine.insertRecord(tuple2.getData());
                        if(scan2.hasNext()){
                            data2 = scan2.getNext(rid2);
                            tuple2 = new Tuple(schema, data2);
                        }else {
                            tuple2 = null;
                        }
                    }
                }

                break;
            case AttrType.FLOAT:
                rid = new RID();
                rid2 = new RID();
                data = null;
                if(scan1.hasNext()) {
                    data = scan1.getNext(rid);
                    tuple = new Tuple(schema, data);
                }
                data2 = null;
                if(scan2.hasNext()) {
                    data2 = scan2.getNext(rid2);
                    tuple2 = new Tuple(schema, data2);
                }
                while(tuple!= null || tuple2 != null) {
                    if(tuple == null){
                        combine.insertRecord(tuple2.getData());
                        if(scan2.hasNext()){
                            data2 = scan2.getNext(rid2);
                            tuple2 = new Tuple(schema, data2);
                        }else {
                            tuple2 = null;
                        }
                        continue;
                    }
                    if(tuple2 == null){
                        combine.insertRecord(tuple.getData());
                        if(scan1.hasNext()){
                            data = scan1.getNext(rid);
                            tuple = new Tuple(schema, data);
                        }else {
                            tuple = null;
                        }
                        continue;
                    }
                    if (tuple.getFloatFld(schemaj) < tuple2.getFloatFld(schemaj)) {
                        combine.insertRecord(tuple.getData());
                        if(scan1.hasNext()){
                            data = scan1.getNext(rid);
                            tuple = new Tuple(schema, data);
                        }else {
                            tuple = null;
                        }
                    }else{
                        combine.insertRecord(tuple2.getData());
                        if(scan2.hasNext()){
                            data2 = scan2.getNext(rid2);
                            tuple2 = new Tuple(schema, data2);
                        }else {
                            tuple2 = null;
                        }
                    }
                }

                break;
        }
        scan1.close();
        scan2.close();
        left.get(0).deleteFile();
        left.get(1).deleteFile();
        left.remove(0);
        left.remove(0);
        left.add(combine);
        return left;
    }

    public HeapFile join(HeapFile left, HeapFile right){
        HeapFile joinData = new HeapFile(null);
        HeapScan scan1 = left.openScan();
        HeapScan scan2 = right.openScan();
        RID rid;
        RID rid2;
        byte[] data = null;
        byte [] data2 = null;
        Tuple tuple = null;
        Tuple tuple2 = null;
        ArrayList<Tuple> dup = new ArrayList<>();
        Tuple prev = null;
        switch (inner.schema.fieldType(this.schemaj)){

            case AttrType.INTEGER:

                //System.out.println("here");
                //System.out.println("k: " + k);
                //System.out.println("l: " + l);
                //System.out.println("size: " + leftTuples.size());
                //System.out.println("size: " + rightTuples.size());
                rid = new RID();
                rid2 = new RID();
                int total = 0;
                if(scan1.hasNext()) {
                    data = scan1.getNext(rid);
                    tuple = new Tuple(outer.schema, data);
                }

                if(scan2.hasNext()) {
                    data2 = scan2.getNext(rid2);
                    tuple2 = new Tuple(inner.schema, data2);
                }
                while(tuple != null && tuple2 != null) {
                    //System.out.println("size: " + leftTuples.size());
                    //System.out.println("k: " + k);
                    //System.out.println("l: " + l);
                    if(prev == tuple){
                        //System.out.println("here");
                        while (!dup.isEmpty()){
                            joinData.insertRecord(Tuple.join(tuple, dup.remove(0), schema).getData());
                        }
                    }
                    if (tuple.getIntFld(this.schemai) == tuple2.getIntFld(this.schemaj)) {
                        joinData.insertRecord(Tuple.join(tuple, tuple2, schema).getData());
                        //count++;
                        dup = new ArrayList<>();
                        prev = tuple;
                        dup.add(tuple2);
                        if(scan2.hasNext()){
                            data2 = scan2.getNext(rid2);
                            tuple2 = new Tuple(inner.schema, data2);
                        }else {
                            tuple2 = null;
                        }
                        for (; tuple2 != null; ) {
                            if (tuple.getIntFld(this.schemai) == tuple2.getIntFld(this.schemaj)) {
                                //count++;
                                joinData.insertRecord(Tuple.join(tuple, tuple2, schema).getData());
                                dup.add(tuple2);
                                if(scan2.hasNext()){
                                    data2 = scan2.getNext(rid2);
                                    tuple2 = new Tuple(inner.schema, data2);
                                }else {
                                    tuple2 = null;
                                }
                            } else {
                                if(tuple.getIntFld(this.schemai) > tuple2.getIntFld(this.schemaj)){
                                    if(scan2.hasNext()){
                                        data2 = scan2.getNext(rid2);
                                        tuple2 = new Tuple(inner.schema, data2);
                                    }else {
                                        tuple2 = null;
                                    }
                                }else{
                                    if(scan1.hasNext()){
                                        data = scan1.getNext(rid);
                                        tuple = new Tuple(outer.schema, data);
                                    }else {
                                        tuple = null;
                                    }
                                }
                                break;
                            }
                        }
                    } else {
                        //System.out.println("tuple1:" + tuple.getIntFld(this.schemai));
                        //System.out.println("tuple2:" + tuple2.getIntFld(this.schemaj));
                        if(tuple.getIntFld(this.schemai) > tuple2.getIntFld(this.schemaj)){
                            if(scan2.hasNext()){
                                data2 = scan2.getNext(rid2);
                                tuple2 = new Tuple(inner.schema, data2);
                            }else {
                                tuple2 = null;
                            }
                        }else{
                            if(scan1.hasNext()){
                                data = scan1.getNext(rid);
                                tuple = new Tuple(outer.schema, data);
                            }else {
                                tuple = null;
                            }
                        }
                    }
                }
                //System.out.println("combine"+ count);
                //System.out.println(countLoop);
                break;
            case AttrType.STRING:
                //System.out.println("here string");
                //System.out.println("k: " + k);
                //System.out.println("l: " + l);
                //System.out.println("size: " + leftTuples.size());
                //System.out.println("size: " + rightTuples.size());
                rid = new RID();
                rid2 = new RID();
                if(scan1.hasNext()) {
                    data = scan1.getNext(rid);
                    tuple = new Tuple(outer.schema, data);
                }
                if(scan2.hasNext()) {
                    data2 = scan2.getNext(rid2);
                    tuple2 = new Tuple(inner.schema, data2);
                }

                while(tuple != null && tuple2 != null) {
                    //System.out.println("size: " + leftTuples.size());
                    //System.out.println("k: " + k);
                    //System.out.println("l: " + l);
                    if(prev == tuple){
                        //System.out.println("here");
                        while (!dup.isEmpty()){
                            joinData.insertRecord(Tuple.join(tuple, dup.remove(0), schema).getData());
                        }
                    }
                    if (tuple.getStringFld(this.schemai).compareTo(tuple2.getStringFld(this.schemaj)) == 0) {
                        joinData.insertRecord(Tuple.join(tuple, tuple2, schema).getData());
                        dup = new ArrayList<>();
                        prev = tuple;
                        dup.add(tuple2);
                        if(scan2.hasNext()){
                            data2 = scan2.getNext(rid2);
                            tuple2 = new Tuple(inner.schema, data2);
                        }else {
                            tuple2 = null;
                        }
                        for (; tuple2 != null; ) {
                            if (tuple.getStringFld(this.schemai).compareTo(tuple2.getStringFld(this.schemaj)) == 0) {
                                joinData.insertRecord(Tuple.join(tuple, tuple2, schema).getData());
                                dup.add(tuple2);
                                if(scan2.hasNext()){
                                    data2 = scan2.getNext(rid2);
                                    tuple2 = new Tuple(inner.schema, data2);
                                }else {
                                    tuple2 = null;
                                }
                            } else {
                                if(tuple.getStringFld(this.schemai).compareTo(tuple2.getStringFld(this.schemaj)) > 0){
                                    if(scan2.hasNext()){
                                        data2 = scan2.getNext(rid2);
                                        tuple2 = new Tuple(inner.schema, data2);
                                    }else {
                                        tuple2 = null;
                                    }
                                }else{
                                    if(scan1.hasNext()){
                                        data = scan1.getNext(rid);
                                        tuple = new Tuple(outer.schema, data);
                                    }else {
                                        tuple = null;
                                    }
                                }
                                break;
                            }
                        }
                    } else {
                        //System.out.println("tuple");
                        if(tuple.getStringFld(this.schemai).compareTo(tuple2.getStringFld(this.schemaj)) > 0){
                            if(scan2.hasNext()){
                                data2 = scan2.getNext(rid2);
                                tuple2 = new Tuple(inner.schema, data2);
                            }else {
                                tuple2 = null;
                            }
                        }else{
                            if(scan1.hasNext()){
                                data = scan1.getNext(rid);
                                tuple = new Tuple(outer.schema, data);
                            }else {
                                tuple = null;
                            }
                        }
                    }
                }
                //System.out.println(count);
                //System.out.println(countLoop);
                break;
            case AttrType.FLOAT:
                rid = new RID();
                rid2 = new RID();

                if(scan1.hasNext()) {
                    data = scan1.getNext(rid);
                    tuple = new Tuple(outer.schema, data);
                }

                if(scan2.hasNext()) {
                    data2 = scan2.getNext(rid2);
                    tuple2 = new Tuple(inner.schema, data2);
                }
                while(tuple != null && tuple2 != null) {
                    //System.out.println("size: " + leftTuples.size());
                    //System.out.println("k: " + k);
                    //System.out.println("l: " + l);
                    if(prev == tuple){
                        //System.out.println("here");
                        while (!dup.isEmpty()){
                            joinData.insertRecord(Tuple.join(tuple, dup.remove(0), schema).getData());
                        }
                    }
                    if (tuple.getFloatFld(this.schemai) == tuple2.getFloatFld(this.schemaj)) {
                        joinData.insertRecord(Tuple.join(tuple, tuple2, schema).getData());
                        dup = new ArrayList<>();
                        prev = tuple;
                        dup.add(tuple2);
                        if(scan2.hasNext()){
                            data2 = scan2.getNext(rid2);
                            tuple2 = new Tuple(inner.schema, data2);
                        }else {
                            tuple2 = null;
                        }
                        for (; tuple2 != null; ) {
                            if (tuple.getFloatFld(this.schemai) == tuple2.getFloatFld(this.schemaj)) {
                                joinData.insertRecord(Tuple.join(tuple, tuple2, schema).getData());
                                dup.add(tuple2);
                                if(scan2.hasNext()){
                                    data2 = scan2.getNext(rid2);
                                    tuple2 = new Tuple(inner.schema, data2);
                                }else {
                                    tuple2 = null;
                                }
                            } else {
                                if(tuple.getFloatFld(this.schemai) > tuple2.getFloatFld(this.schemaj)){
                                    if(scan2.hasNext()){
                                        data2 = scan2.getNext(rid2);
                                        tuple2 = new Tuple(inner.schema, data2);
                                    }else {
                                        tuple2 = null;
                                    }
                                }else{
                                    if(scan1.hasNext()){
                                        data = scan1.getNext(rid);
                                        tuple = new Tuple(outer.schema, data);
                                    }else {
                                        tuple = null;
                                    }
                                }
                                break;
                            }
                        }
                    } else {
                        if(tuple.getFloatFld(this.schemai) > tuple2.getFloatFld(this.schemaj)){
                            if(scan2.hasNext()){
                                data2 = scan2.getNext(rid2);
                                tuple2 = new Tuple(inner.schema, data2);
                            }else {
                                tuple2 = null;
                            }
                        }else{
                            if(scan1.hasNext()){
                                data = scan1.getNext(rid);
                                tuple = new Tuple(outer.schema, data);
                            }else {
                                tuple = null;
                            }
                        }
                    }
                }
                //System.out.println(count);
                //System.out.println(countLoop);
                break;
        }
        scan1.close();
        return joinData;
    }

    /**
     * Gives a one-line explaination of the iterator, repeats the call on any
     * child iterators, and increases the indent depth along the way.
     */
    public void explain(int depth) {

        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Restarts the iterator, i.e. as if it were just constructed.
     */
    public void restart() {
        outer.restart();
        inner.restart();
        //throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Returns true if the iterator is open; false otherwise.
     */
    public boolean isOpen() {
        return (outer.isOpen() && inner.isOpen());
        //throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Closes the iterator, releasing any resources (i.e. pinned pages).
     */
    public void close() {
        inner.close();
        outer.close();
        //throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Returns true if there are more tuples, false otherwise.
     */
    public boolean hasNext() {
        if (scan.hasNext()){
            return true;
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
        if(hasNext()){
            //System.out.println("here");
            RID rid = new RID();
            byte[] data = scan.getNext(rid);
            if(data == null){
                throw new IllegalStateException("no tuples left");
            }else {
                return new Tuple(schema, data);
            }
        }
        throw new IllegalStateException("no tuples left");
    }

}
