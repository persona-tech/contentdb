package com.discovery.contentdb.matrix;

import com.discovery.contentdb.matrix.solrj.tv.TermVectorResponse;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.math.AbstractMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.TermVectorParams;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * {@author} gcapan Casts a Solr Field to a read-only matrix, where one document represents a row, and indexed by the
 * idField.
 */
public class SolrFieldMatrix extends AbstractMatrix {
  private String idField;
  private String field;
  private TYPE type;
  private HttpSolrServer server;
  private int rows;
  private String[] columnLabels;
  private Map<String, Integer> columnLabelBindings;

  public SolrFieldMatrix(String url, String idField, String field, TYPE type) throws IOException,
    SolrServerException {
    super(Integer.MAX_VALUE, 0);
    this.idField = idField;
    this.field = field;
    this.type = type;
    this.server = new HttpSolrServer(url);
    server.setMaxRetries(1);
    server.setConnectionTimeout(2000);
    server.setAllowCompression(true);
    initialize();
  }

  private void initialize() throws IOException, SolrServerException {
    int columns = 0;
    columnLabelBindings = Maps.newHashMap();
    if (type.equals(TYPE.BOOLEAN) || type.equals(TYPE.NUMERICAL)) {
      columnLabelBindings.put(field, 0);
      columns = 1;
    } else if (type.equals(TYPE.TEXT) || type.equals(TYPE.MULTINOMIAL)) {
      LukeRequest lukeRequest = new LukeRequest();
      lukeRequest.setNumTerms(10000);
      lukeRequest.setFields(Lists.newArrayList(field));
      lukeRequest.setMethod(SolrRequest.METHOD.GET);

      final LukeResponse response = lukeRequest.process(server);
      int i = 0;
      for (Map.Entry<String, Integer> histogramEntry : response.getFieldInfo().get(field).getTopTerms()) {
        String word = histogramEntry.getKey();
        columnLabelBindings.put(word, i++);

      }
      columns = i;
    }
    this.columns = columns;
  }

  @Override
  public int columnSize() {
    return this.columns;
  }

  public FastIDSet getCandidates(String keyword, int maxLength) throws SolrServerException {
    SolrQuery query = new SolrQuery();
    query.setFacet(false).
      setHighlight(false);

    if (!(type == TYPE.TEXT)) {
      if (type == TYPE.BOOLEAN) {
        keyword = "true";
      }
      query.setQuery(field + ":" + keyword);
    } else {
      query.setQuery(keyword);
      query.setParam(CommonParams.DF, this.field);
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
    for (SolrDocument document : docs) {
      String id = (String) document.getFieldValue(idField);
      idSet.add(Long.parseLong(id));
    }
    return idSet;
  }

  private SolrDocumentList getDocuments(String keyword, int maxLength) throws SolrServerException {
    SolrQuery query = new SolrQuery();
    query.setFacet(false).
      setHighlight(false).
      setRows(maxLength);

    if (!(type == TYPE.TEXT)) {
      if (type == TYPE.BOOLEAN) {
        keyword = "true";
      }
      query.setQuery(field + ":" + keyword);
    } else {
      query.setQuery(keyword);
    }
    query.setFields(idField, field);
    return server.query(query).getResults();
  }

  private SolrDocument viewDocument(int docId) throws SolrServerException {
    SolrQuery query = new SolrQuery();
    query.setFacet(false).
      setHighlight(false).
      setRows(1).
      setFields(idField, field).
      setQuery(idField + ":" + docId);
    SolrDocumentList results = server.query(query).getResults();
    if (results.size() == 0) {
      return null;
    } else {
      return results.get(0);
    }
  }

  private List<TermVectorResponse.TermVectorInfo> viewTerms(int docId) throws SolrServerException {
    SolrQuery query = new SolrQuery();
    query.setRows(1).
      setFields(idField, field).
      setParam(CommonParams.DF, this.field).
      setParam(TermVectorParams.TF_IDF, true).
      setParam(TermVectorParams.DF, true).
      setParam(TermVectorParams.TF, true).
      setParam(TermVectorParams.FIELDS, field).
      setIncludeScore(false).
      setQuery(idField + ":" + docId);
    System.out.println(query);
    QueryResponse queryResponse = server.query(query);
    System.out.println(queryResponse);
    TermVectorResponse termVectorResponse = new TermVectorResponse(query, queryResponse);
    return termVectorResponse.getTermVectorInfoList();
  }

  public Vector viewRow(int row) {
    Vector v = new SequentialAccessSparseVector(columnSize());
    if (type == TYPE.NUMERICAL) {
      SolrDocument document = null;
      try {
        document = viewDocument(row);
      } catch (SolrServerException e) {
        return null;
      }
      if (document != null) {
        v.setQuick(0, Double.parseDouble((String) document.getFieldValue(field)));
      }
      return v;
    } else if (type == TYPE.BOOLEAN) {
      return null;
    } else if (type == TYPE.MULTINOMIAL) {
      SolrDocument document = null;
      try {
        document = viewDocument(row);
      } catch (SolrServerException e) {
        return null;
      }
      if (document != null) {
        v.setQuick(columnLabelBindings.get((String) document.getFieldValue(field)), 1);
      }
      return v;
    } else if (type == TYPE.TEXT) {

      List<TermVectorResponse.TermVectorInfo> terms = null;
      try {
        terms = viewTerms(row);
      } catch (SolrServerException e) {
        return null;
      }
      for (TermVectorResponse.TermVectorInfo term : terms) {
        String word = term.getWord();
        v.setQuick(columnLabelBindings.get(word), term.getTfIdf());
      }
      return v;
    } else {
      return null;
    }
  }

  @Override
  public Vector viewColumn(int column) {
    String keyword = columnLabelBindings.keySet().toArray(new String[columnLabelBindings.size()])[column];
    try {
      FastIDSet idSet = getCandidates(keyword, rowSize());
      Vector v = new SequentialAccessSparseVector(rowSize());
      for (long id : idSet) {
        v.setQuick((int) id, get((int) id, column));
      }
      return v;
    } catch (SolrServerException se) {
      return null;
    }
  }

  @Override
  public Matrix assignColumn(int column, Vector other) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Matrix assignRow(int row, Vector other) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getQuick(int row, int column) {
    return viewRow(row).getQuick(column);
  }

  @Override
  public Matrix like() {
    return new SparseMatrix(rowSize(), columnSize());
  }

  @Override
  public Matrix like(int rows, int columns) {
    return new SparseMatrix(rows, columns);
  }

  @Override
  public void setQuick(int row, int column, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Matrix viewPart(int[] offset, int[] size) {
    //TODO: this SHOULD be supported, actually
    throw new UnsupportedOperationException();
  }

  public static void main(String[] args) throws Exception {
    //example usage
    SolrFieldMatrix matrix = new SolrFieldMatrix("http://localhost:8983/solr", "id", "includes", TYPE.TEXT);
    //matrix.getCandidates("myKeyword", 5);
    matrix.viewTerms(4);
    //matrix.viewRow(2);
  }
}
