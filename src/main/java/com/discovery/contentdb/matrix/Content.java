package com.discovery.contentdb.matrix;

import com.discovery.contentdb.matrix.exception.ContentException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.math.Matrix;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrInputDocument;

/**
 * {@author} gcapan
 * A com.discovery.contentdb.matrix.Content is a matrix, with additional querying, spatial filtering, and most similars functionality; which can
 * be used to store user demographics information, and item content information that would be beneficial an online
 * recommender system; or any other system that needs to treat a content database, a document index,
 * for example, as a matrix.
 */
public interface Content extends Matrix {

  /**
   * Used for pre-filtering entities based on a keyword
   * @param keyword The keyword/category to be searched
   * @param maxLength max number of candidates to be returned
   * @return set of candidate entities
   */
  FastIDSet getCandidates (String identifier, String keyword, int maxLength) throws ContentException;

  FastIDSet getCandidates (String identifier, String keyword, int start, int maxLength) throws ContentException;


  /**
   * like {@link Content#getCandidates(String, String, int)}, with an extension of spatial filtering
   * Used for pre-filtering entities within a proximity, based on a keyword
   * @param keyword The keyword to be searched (or category to be matched)
   * @param latitude Reference latitude information
   * @param longitude Reference longitude information
   * @param rangeInKm Range the results within, in kilometers
   * @return set of candidate entities
   */
  FastIDSet getCandidates (String identifier, String keyword, double latitude, double longitude,
                           int rangeInKm)throws ContentException;

  /**
   * Used for pre-filtering entities based on a {@link SolrQuery}, if supported.
   * @param query The query
   * @param maxLength max number of candidates to be returned
   * @return set of candidate entities
   */
  FastIDSet getCandidates (String identifier, SolrQuery query, int maxLength) throws ContentException;

  FastIDSet getCandidates (String identifier, SolrQuery query, int start, int maxLength) throws ContentException;

  /**
   * Used to find most similar entities in content.
   * @param id Id of the entity to find most similars for
   * @param maxLength max number of candidates to be returned
   * @return set of most similar entities
   */
  FastIDSet mostSimilars (String identifier, int id, int maxLength) throws ContentException;

  /**
   * Used to add a new entity
   * @param id id of the entity
   * @param document the content vector encoded as {@link SolrInputDocument}
   * @throws ContentException
   */
  void setContent(int id, SolrInputDocument document) throws ContentException;


}
