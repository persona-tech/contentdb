package com.discovery.contentdb.matrix;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class SolrFieldMatrixTest {

  private EmbeddedSolrServer solrServer;

  @Before
  public void initializeServer() throws IOException, SolrServerException {

    System.out.println(new File(".").getAbsolutePath());
    String solrHome = "target/test-classes/solr-home";
    CoreContainer container = new CoreContainer(solrHome);
    container.load(solrHome, new File(solrHome + "/solr.xml"));

    solrServer = new EmbeddedSolrServer(container, "fieldmatrix");

    // Even though it is Embedded, instance data directory is persisted between restarts. So we should empty the index before the tests.
    solrServer.deleteByQuery("*:*");
    solrServer.commit();

    populateServerWithData();
  }

  private void populateServerWithData() throws SolrServerException, IOException {
    SolrInputDocument doc1 = new SolrInputDocument();
    doc1.addField("id", "1");
    doc1.addField("textField", "Ali okula geldi.");

    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.addField("id", "2");
    doc2.addField("textField", "Veli okuldan dondu.");

    Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    docs.add(doc1);
    docs.add(doc2);

    solrServer.add(docs);
    solrServer.commit();
  }

  @Test
  public void getTermVectors() throws IOException, SolrServerException {

    SolrFieldMatrix matrix = new SolrFieldMatrix(solrServer, "id", "textField", TYPE.TEXT);
    System.out.println(matrix.viewTerms(1));
  }
}
