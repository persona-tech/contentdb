package com.personaissance.persona.contentdb.matrix;

import com.personaissance.persona.contentdb.Content;
import com.personaissance.persona.contentdb.exception.ContentException;
import com.google.common.collect.Maps;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrInputDocument;

import java.util.Map;

/**
 * {@author} gcapan
 * A collection of {@link SolrFieldMatrix}es.
 */
public class SolrMatrix extends SuperMatrix implements Content {
  private Map<String, Integer> fieldMappings = Maps.newHashMap();
  private SolrFieldMatrix[] matrices;
  public SolrMatrix(SolrFieldMatrix[] matrices) {
    super(matrices);
    this.matrices = matrices;
    for(int i = 0; i<matrices.length; i++){
      SolrFieldMatrix matrix = matrices[i];
      fieldMappings.put(matrix.getFieldName(), i);
    }
  }

  private void assignRow(int id, SolrInputDocument document) throws ContentException{
    matrices[0].assignRow(id, document);
  }

  @Override
  public FastIDSet getCandidates(String identifier, String keyword, int maxLength) throws ContentException {
    return matrices[fieldMappings.get(identifier)].getCandidates(keyword, maxLength);
  }

  @Override
  public FastIDSet getCandidates(String identifier, String keyword, int start, int maxLength) throws ContentException {
    return matrices[fieldMappings.get(identifier)].getCandidates(keyword, start, maxLength);
  }

  @Override
  public FastIDSet getCandidates(String identifier, String keyword, double latitude, double longitude, int rangeInKm)
     throws ContentException {
    return matrices[fieldMappings.get(identifier)].getCandidates(keyword, latitude, longitude, rangeInKm);
  }

  @Override
  public FastIDSet getCandidates(String identifier, SolrQuery query, int maxLength) throws ContentException {
    return matrices[fieldMappings.get(identifier)].getCandidates(query, maxLength);
  }

  @Override
  public FastIDSet getCandidates(String identifier, SolrQuery query, int start, int maxLength) throws ContentException {
    return matrices[fieldMappings.get(identifier)].getCandidates(query, start, maxLength);
  }

  @Override
  public FastIDSet mostSimilars(String identifier, int id, int maxLength) throws ContentException {
    return matrices[fieldMappings.get(identifier)].mostSimilars(id, maxLength);
  }

  @Override
  public void setContent(int id, SolrInputDocument document) throws ContentException {
    assignRow(id, document);
  }
}
