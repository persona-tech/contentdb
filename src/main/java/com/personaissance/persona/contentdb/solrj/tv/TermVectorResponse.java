package com.personaissance.persona.contentdb.solrj.tv;

import com.google.common.collect.Lists;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.TermVectorParams;
import org.apache.solr.common.util.NamedList;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author gcapan
 */
public class TermVectorResponse {
  public List<TermVectorInfo> getTermVectorInfoList() {
    return termVectorInfoList;
  }

  List<TermVectorInfo> termVectorInfoList = Lists.newArrayList();

  //assumes one tv.fl, creates a list of term entries, to each of which is attached the tf, df,
  // and tf_idf values if available
  public TermVectorResponse(SolrQuery query, QueryResponse solrResponse, String id) {
    NamedList namedList = solrResponse.getResponse();
    boolean tf = Boolean.parseBoolean(query.get(TermVectorParams.TF));
    boolean df = Boolean.parseBoolean(query.get(TermVectorParams.DF));
    boolean tf_idf = Boolean.parseBoolean(query.get(TermVectorParams.TF_IDF));

    NamedList termVectorsList = (NamedList) namedList.get("termVectors");
    NamedList listForDoc = (NamedList) termVectorsList.get(id);
    NamedList fieldTermsList = (NamedList) listForDoc.get(query.get(TermVectorParams.FIELDS));

    Iterator<Map.Entry<String, Object>> tvInfoIterator = fieldTermsList.iterator();
    while (tvInfoIterator.hasNext()) {
      Map.Entry<String, Object> tvInfo = tvInfoIterator.next();
      String word = tvInfo.getKey();
      NamedList tv = (NamedList) tvInfo.getValue();
      TermVectorInfo termVectorInfo = new TermVectorInfo(word);
      if (tf) {
        termVectorInfo.setTf(Integer.parseInt(tv.get("tf").toString()));
      }
      if (df) {
        termVectorInfo.setDf(Integer.parseInt(tv.get("df").toString()));
      }
      if (tf_idf) {
        termVectorInfo.setTfIdf(Double.parseDouble(tv.get("tf-idf").toString()));
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

    @Override
    public String toString() {
      return word+":{" +
        "tf=" + tf +
        ", df=" + df +
        ", tfIdf=" + tfIdf +
        '}';
    }
  }
}
