package com.discovery.contentdb.matrix;

import com.discovery.contentdb.matrix.exception.ContentException;
import com.discovery.contentdb.matrix.solrj.tv.TermVectorResponse;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.math.*;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
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
  private String spatialField = null;
  private boolean multivalued;
  private TYPE type;
  private SolrServer server;
  private int rows;
  private Map<String, Integer> columnLabelBindings;

  public SolrFieldMatrix(SolrServer server, String idField, String field, TYPE type,
                         boolean multivalued) throws IOException, SolrServerException {
    super(Integer.MAX_VALUE, 0);
    this.multivalued = multivalued;
    if (this.multivalued) {
      Preconditions.checkArgument(type.equals(TYPE.MULTINOMIAL), "Multivalued is not supported unless this is a " +
         "multinomial field");
    }
    this.idField = idField;
    this.field = field;
    this.type = type;
    this.server = server;
    initialize();
  }

  public SolrFieldMatrix(SolrServer server, String idField, String field, boolean multivalued, String spatialField,
                         TYPE type) throws IOException,
     SolrServerException {
    super(Integer.MAX_VALUE, 0);
    this.idField = idField;
    this.field = field;
    this.type = type;
    this.server = server;
    this.spatialField = spatialField;
    this.multivalued = multivalued;
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
      lukeRequest.setNumTerms(1000);
      lukeRequest.setFields(Lists.newArrayList(field));
      lukeRequest.setMethod(SolrRequest.METHOD.GET);


      final LukeResponse response = lukeRequest.process(server);
      int i = 0;
      for (Map.Entry<String, Integer> histogramEntry : response.getFieldInfo().get(field).getTopTerms()) {
        String word = histogramEntry.getKey();
        columnLabelBindings.put(word, i++);

      }
      columns = i;
      setColumnLabelBindings(columnLabelBindings);
    }
    this.columns = columns;
  }

  public String getFieldName(){
    return field;
  }
  @Override
  public int columnSize() {
    return this.columns;
  }

  public FastIDSet getCandidates(String keyword, int maxLength)  throws ContentException{
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

  public FastIDSet getCandidates(String keyword, double latitude, double longitude, int rangeInKm) throws ContentException {
    Preconditions.checkNotNull(spatialField, "You should determine the spatial field in your Solr index");
    SolrQuery query = new SolrQuery();
    query.setQuery(field + ":" + keyword);
    query.setParam("fq", "{!bbox}");
    query.setParam("sfield", spatialField);
    query.setParam("pt", latitude + "," + longitude);
    query.setParam("d", Integer.toString(rangeInKm));
    try {
      return getCandidates(query);
    } catch (SolrServerException se) {
      throw new ContentException(se);
    }
  }

  public FastIDSet getCandidates(SolrQuery query, int maxLength) throws ContentException {
    query.setRows(maxLength).
       setStart(0);
    try {
      return getCandidates(query);
    } catch (SolrServerException se) {
      throw  new ContentException(se);
    }
  }

  private FastIDSet getCandidates(SolrQuery query) throws SolrServerException {
    FastIDSet idSet = new FastIDSet();
    query.setFields(idField);
    SolrDocumentList docs = server.query(query).getResults();
    for (SolrDocument document : docs) {
      String id = String.valueOf(document.getFieldValue(idField));
      idSet.add(Long.parseLong(id));
    }
    return idSet;
  }

  public FastIDSet mostSimilars(int docId, int maxLength) throws ContentException {
    SolrQuery query = new SolrQuery();
    query.setRequestHandler("/mlt").
       setQuery(idField + ":" + docId).
       setParam("mlt.fl", field).
       setStart(0).
       setRows(maxLength);

    try {
      return getCandidates(query);
    } catch (SolrServerException e) {
      throw new ContentException(e);
    }
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
       setRequestHandler("/tvrh").
       setQuery(idField + ":" + docId);
    QueryResponse queryResponse = server.query(query);
    TermVectorResponse termVectorResponse = new TermVectorResponse(query, queryResponse, Integer.toString(docId));
    return termVectorResponse.getTermVectorInfoList();
  }

  public Vector viewRow(int row) {
    Vector v = new SequentialAccessSparseVector(columnSize());
    SolrDocument document = null;
    try {
      document = viewDocument(row);
    } catch (SolrServerException e) {
      return null;
    }

    if (type == TYPE.NUMERICAL) {
      if (document != null) {
        v.setQuick(0, ((Number) document.getFieldValue(field)).doubleValue());
      }
      return v;
    } else if (type == TYPE.BOOLEAN) {
      if (document != null) {
        v.setQuick(0, (Boolean) document.getFieldValue(field) ? 1 : 0);
        return v;
      }
    } else if (type == TYPE.MULTINOMIAL) {
      if (document != null) {
        String fieldValue = document.getFieldValue(field).toString();
        if (multivalued) {
          String[] words = fieldValue.substring(1, fieldValue.lastIndexOf(']')).split(",\\s*");
          for (String word : words) {
            v.setQuick(columnLabelBindings.get(word), 1);
          }
        } else {
          v.setQuick(columnLabelBindings.get(fieldValue), 1);
        }
        return v;
      }
    } else if (type == TYPE.TEXT) {
      List<TermVectorResponse.TermVectorInfo> terms;
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
    return null;
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
    } catch (ContentException ce) {
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
  }
}
