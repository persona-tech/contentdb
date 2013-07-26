package com.discovery.contentdb.matrix;

import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.math.*;
import org.apache.mahout.math.function.DoubleDoubleFunction;
import org.apache.mahout.math.function.DoubleFunction;
import org.apache.mahout.math.function.VectorFunction;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;

import java.util.Iterator;
import java.util.Map;

/**
 * {@author} gcapan
 */
public class SolrMatrix implements Matrix {
  private String idField;
  private String[] fields;
  private TYPE[] types;
  private HttpSolrServer server;
  private int rows;
  private int columns;

  public SolrMatrix(String url){
    this.server = new HttpSolrServer(url);
    server.setMaxRetries(1);
    server.setConnectionTimeout(2000);
    server.setAllowCompression(true);
    initialize();
  }

  private void initialize(){
    columns = 0;
    int i = 0;
    for(String field:fields){
      if(types[i].equals(TYPE.BOOLEAN)||types[i].equals(TYPE.NUMERICAL)){
        columns++;
      } else if(types[i].equals(TYPE.MULTINOMIAL)){

      } else if(types[i].equals(TYPE.TEXT)){

      }
    }
    i++;
  }

  public FastIDSet getCandidates(String keyword, int field, TYPE type, int maxLength) throws SolrServerException{
    SolrQuery query = new SolrQuery();
    query.setFacet(false).
       setHighlight(false);

    if(!(type == TYPE.TEXT)){
      if(type == TYPE.BOOLEAN){
        keyword = "true";
      }
      query.setQuery("{!term f="+field+"}"+keyword);
    } else {
      query.setQuery(keyword);
      query.setParam(CommonParams.DF, fields[field]);
    }
    return getCandidates(query, maxLength);
  }

  public FastIDSet getCandidates(SolrQuery query, int maxLength) throws SolrServerException {
    query.setRows(maxLength).
       setStart(0);
    return getCandidates(query);
  }

  private FastIDSet getCandidates(SolrQuery query) throws SolrServerException {
    FastIDSet idSet = new FastIDSet(query.getRows());
    query.setFields(idField);
    SolrDocumentList docs = server.query(query).getResults();
    for(SolrDocument document:docs) {
      String id = (String)document.getFieldValue(idField);
      idSet.add(Long.parseLong(id));
    }
    return idSet;
  }


  @Override
  public String asFormatString() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix assign(double v) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix assign(double[][] doubles) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix assign(Matrix matrixSlices) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix assign(DoubleFunction doubleFunction) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix assign(Matrix matrixSlices, DoubleDoubleFunction doubleDoubleFunction) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix assignColumn(int i, Vector vector) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix assignRow(int i, Vector vector) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Vector aggregateRows(VectorFunction vectorFunction) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Vector aggregateColumns(VectorFunction vectorFunction) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public double aggregate(DoubleDoubleFunction doubleDoubleFunction, DoubleFunction doubleFunction) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int columnSize() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int rowSize() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix clone() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public double determinant() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix divide(double v) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public double get(int i, int i2) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public double getQuick(int i, int i2) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix like() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix like(int i, int i2) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix minus(Matrix matrixSlices) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix plus(double v) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix plus(Matrix matrixSlices) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void set(int i, int i2, double v) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void set(int i, double[] doubles) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void setQuick(int i, int i2, double v) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int[] getNumNondefaultElements() {
    return new int[0];  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix times(double v) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix times(Matrix matrixSlices) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix transpose() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public double zSum() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Map<String, Integer> getColumnLabelBindings() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Map<String, Integer> getRowLabelBindings() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void setColumnLabelBindings(Map<String, Integer> stringIntegerMap) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void setRowLabelBindings(Map<String, Integer> stringIntegerMap) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public double get(String s, String s2) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void set(String s, String s2, double v) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void set(String s, String s2, int i, int i2, double v) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void set(String s, double[] doubles) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void set(String s, int i, double[] doubles) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix viewPart(int[] ints, int[] ints2) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Matrix viewPart(int i, int i2, int i3, int i4) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Vector viewRow(int i) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Vector viewColumn(int i) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Vector viewDiagonal() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Iterator<MatrixSlice> iterateAll() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int numSlices() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int numRows() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int numCols() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Vector times(Vector vector) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Vector timesSquared(Vector vector) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Iterator<MatrixSlice> iterator() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
