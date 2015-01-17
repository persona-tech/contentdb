package com.personaissance.persona.contentdb.matrix;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import org.apache.mahout.math.AbstractVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.OrderedIntDoubleMapping;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.SparseRowMatrix;
import org.apache.mahout.math.Vector;

import java.util.ArrayList;
import java.util.Iterator;


public class VectorSuperView extends AbstractVector {
  Vector[] vectors;
  int[] sizes;
  ArrayList<Integer> indices = Lists.newArrayList();
  ArrayList<Double> values = Lists.newArrayList();

  public VectorSuperView(int cardinality, Vector[] vectors) {
    super(cardinality);
    this.vectors = vectors;
    sizes = new int[vectors.length];
    for (int i = 0; i < vectors.length; i++) {
      sizes[i] = vectors[i].size();
    }

    int offset = 0;
    for (int i = 0; i < vectors.length; i++) {
      for (Vector.Element element : vectors[i].nonZeroes()) {
        indices.add(element.index() + offset);
        values.add(element.get());
      }
      offset += sizes[i];
    }
  }

  public static Vector createNewVector(int cardinality, Vector[] vectors) {
    Vector v = new SequentialAccessSparseVector(cardinality);
    int offset = 0;
    for (int i = 0; i < vectors.length; i++) {
      for (Vector.Element element : vectors[i].nonZeroes()) {
        v.setQuick(element.index() + offset, element.get());
      }
    }
    return v;
  }

  @Override
  public int getNumNondefaultElements() {
    return indices.size();
  }

  @Override
  public double getLookupCost() {
    double avg = 0;
    for (Vector v : vectors) {
      avg += v.getLookupCost();
    }
    return avg / (double) vectors.length;
  }

  @Override
  public double getIteratorAdvanceCost() {
    double avg = 0;
    for (Vector v : vectors) {
      avg += v.getIteratorAdvanceCost();
    }
    return avg / (double) vectors.length;
  }

  @Override
  public boolean isAddConstantTime() {
    return false;
  }

  @Override
  public boolean isDense() {
    return false;
  }

  @Override
  public boolean isSequentialAccess() {
    return true;
  }

  @Override
  public void mergeUpdates(OrderedIntDoubleMapping updates) {
    int noUpdates = updates.getNumMappings();
    int[] indices = updates.getIndices();
    double[] values = updates.getValues();

    for (int i = 0; i < noUpdates; i++) {
      int[] vectorAndColumn = getVectorAndColumn(indices[i]);
      this.vectors[vectorAndColumn[0]].setQuick(vectorAndColumn[1], values[i]);
    }
  }

  @Override
  public Iterator<Element> iterator() {
    return new AllIterator();
  }

  @Override
  public Iterator<Element> iterateNonZero() {
    return new SequentialIterator();
  }

  @Override
  protected Matrix matrixLike(int rows, int columns) {
    return new SparseRowMatrix(rows, columns);
  }

  private final class RandomAccessIterator extends AbstractIterator<Element> {
    private final RandomAccessElement element;

    private int offset;

    private RandomAccessIterator() {
      this.element = new RandomAccessElement();
    }

    @Override
    protected Element computeNext() {
      if (offset >= indices.size()) {
        return endOfData();
      }
      element.index = indices.get(offset);
      offset++;
      return element;
    }

  }

  private final class SequentialIterator extends AbstractIterator<Element> {

    private final NonDefaultElement element = new NonDefaultElement();

    @Override
    protected Element computeNext() {
      int size = values.size();
      if (size <= 0 || element.getNextOffset() >= size) {
        return endOfData();
      }
      element.advanceOffset();
      return element;
    }

  }


  private final class AllIterator extends AbstractIterator<Element> {

    private AllIterator() {
      element.index = -1;
    }

    private final RandomAccessElement element = new RandomAccessElement();

    @Override
    protected Element computeNext() {
      if (element.index + 1 < size()) {
        element.index++;
        return element;
      } else {
        return endOfData();
      }
    }

  }

  private final class RandomAccessElement implements Element {

    int index;

    @Override
    public double get() {
      return getQuick(index);
    }

    @Override
    public int index() {
      return index;
    }

    @Override
    public void set(double value) {
      invalidateCachedLength();
      if (value == 0.0) {
        setQuick(index, value);
      }
    }
  }

  private final class NonDefaultElement implements Element {

    private int offset = -1;

    void advanceOffset() {
      offset++;
    }

    int getNextOffset() {
      return offset + 1;
    }

    @Override
    public double get() {
      return values.get(offset);
    }

    @Override
    public int index() {
      return indices.get(offset);
    }

    @Override
    public void set(double value) {
      invalidateCachedLength();
      setQuick(index(), value);
    }
  }

  @Override
  public double getQuick(int index) {
    int[] vectorAndColumn = getVectorAndColumn(index);
    return vectors[vectorAndColumn[0]].getQuick(vectorAndColumn[1]);
  }

  @Override
  public Vector like() {
    Vector[] vectors = new Vector[this.vectors.length];
    for (int i = 0; i < vectors.length; i++) {
      vectors[i] = this.vectors[i].like();
    }
    return new VectorSuperView(this.size(), vectors);
  }

  @Override
  public void setQuick(int index, double value) {
    if (value != 0 && !indices.contains(index)) {
      indices.add(index);
      values.add(value);
    }
    int[] vectorAndColumn = getVectorAndColumn(index);
    vectors[vectorAndColumn[0]].setQuick(vectorAndColumn[1], value);
  }

  private int[] getVectorAndColumn(int index) {
    int i = 0;

    while (index >= sizes[i]) {
      index -= sizes[i++];
    }
    return new int[]{i, index};
  }
}