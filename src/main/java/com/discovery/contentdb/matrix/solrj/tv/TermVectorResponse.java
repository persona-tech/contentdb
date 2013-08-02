package com.discovery.contentdb.matrix.solrj.tv;

import com.google.common.collect.Lists;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.TermVectorParams;
import org.apache.solr.common.util.NamedList;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * User: gcapan Date: 8/2/13 Time: 3:48 AM
 */
public class TermVectorResponse {
  public List<TermVectorInfo> getTermVectorInfoList() {
    return termVectorInfoList;
  }

  List<TermVectorInfo> termVectorInfoList = Lists.newArrayList();

  //assumes one document, one tv.fl
  public TermVectorResponse(SolrQuery query, QueryResponse solrResponse) {
    NamedList namedList = solrResponse.getResponse();
    boolean tf = Boolean.parseBoolean(query.get(TermVectorParams.TF));
    boolean df = Boolean.parseBoolean(query.get(TermVectorParams.DF));
    boolean tf_idf = Boolean.parseBoolean(query.get(TermVectorParams.TF_IDF));

    NamedList fieldTermsList = (NamedList) ((NamedList<NamedList>) namedList.get
      ("termVectors"))
      .iterator()
      .next()
      .getValue()
      .get(query.get(TermVectorParams.FIELDS));

    Iterator<Map.Entry<String, Object>> tvInfoIterator = fieldTermsList.iterator();
    while (tvInfoIterator.hasNext()) {
      Map.Entry<String, Object> tvInfo = tvInfoIterator.next();
      String word = tvInfo.getKey();
      NamedList tv = (NamedList) tvInfo.getValue();
      TermVectorInfo termVectorInfo = new TermVectorInfo(word);
      if (tf) {
        termVectorInfo.setTf(Integer.parseInt((String) tv.get("tf")));
      }
      if (df) {
        termVectorInfo.setDf(Integer.parseInt((String) tv.get("df")));
      }
      if (tf_idf) {
        termVectorInfo.setTfIdf(Double.parseDouble((String) tv.get("tf-idf")));
      }
      termVectorInfoList.add(termVectorInfo);
    }
  }

  public static class TermVectorInfo {
    int tf = 0;
    int df = 0;
    double tfIdf = 0;
    String word;

    public TermVectorInfo(String word) {
      this.word = word;
    }

    public String getWord() {
      return word;
    }

    public int getTf() {
      return tf;
    }

    public void setTf(int tf) {
      this.tf = tf;
    }

    public int getDf() {
      return df;
    }

    public void setDf(int df) {
      this.df = df;
    }

    public double getTfIdf() {
      return tfIdf;
    }

    public void setTfIdf(double tfIdf) {
      this.tfIdf = tfIdf;
    }
  }
}
