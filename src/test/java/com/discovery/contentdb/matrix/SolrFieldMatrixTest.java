package com.discovery.contentdb.matrix;

import com.discovery.contentdb.matrix.exception.ContentException;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class SolrFieldMatrixTest {

  private EmbeddedSolrServer solrServer;
  private CoreContainer container;

  @Before
  public void initializeServer() throws IOException, SolrServerException {
    String solrHome = "src/test/resources/solr-home";
    container = new CoreContainer(solrHome);
    container.load(solrHome, new File(solrHome + "/solr.xml"));

    solrServer = new EmbeddedSolrServer(container, "fieldmatrix");
    populateServerWithData();
  }

  private void populateServerWithData() throws SolrServerException, IOException {
    SolrInputDocument doc1 = new SolrInputDocument();
    doc1.addField("id", "1");
    doc1.addField("textField", "Sentence one.");
    doc1.addField("intField", "10");
    doc1.addField("boolField", "true");
    doc1.addField("naryStringField", "cat1");
    doc1.addField("multiNaryStringField", "cat1");
    doc1.addField("multiNaryStringField", "cat2");
    doc1.addField("loc", "38.424546,27.13034"); // Izmir

    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.addField("id", "2");
    doc2.addField("textField", "Sentence two.");
    doc2.addField("intField", "20");
    doc2.addField("boolField", "false");
    doc2.addField("naryStringField", "cat2");
    doc2.addField("multiNaryStringField", "cat2");
    doc2.addField("multiNaryStringField", "cat3");
    doc2.addField("loc", "39.937119,32.852447"); // Ankara


    SolrInputDocument doc3 = new SolrInputDocument();
    doc3.addField("id", "3");
    doc3.addField("textField", "Sentence three.");
    doc3.addField("intField", "30");
    doc3.addField("boolField", "true");
    doc3.addField("naryStringField", "cat3");
    doc3.addField("multiNaryStringField", "cat3");
    doc3.addField("multiNaryStringField", "cat1");
    doc3.addField("loc", "41.018765,28.977706"); // Istanbul

    SolrInputDocument doc4 = new SolrInputDocument();
    doc4.addField("id", "4");
    doc4.addField("textField", "Sentence three.");
    doc4.addField("intField", "30");
    doc4.addField("boolField", "true");
    doc4.addField("naryStringField", "cat3");
    doc4.addField("multiNaryStringField", "cat3");
    doc4.addField("multiNaryStringField", "cat1");

    Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    docs.add(doc1);
    docs.add(doc2);
    docs.add(doc3);

    solrServer.add(docs);
    solrServer.commit();
  }

  @Test
  public void testSpatialSearch() throws IOException, SolrServerException, ContentException {
    SolrFieldMatrix matrix = new SolrFieldMatrix(solrServer, "id", "textField", false, "loc", TYPE.TEXT);
    assertEquals(1, matrix.getCandidates("Sentence", 36.887584, 30.692657, 9000).iterator().peek());
  }

  @Test
  public void testColumnSize() throws Exception {
    SolrFieldMatrix matrix = new SolrFieldMatrix(solrServer, "id", "textField", TYPE.TEXT, false);
    assertEquals(4, matrix.columnSize());
    SolrFieldMatrix matrix2 = new SolrFieldMatrix(solrServer, "id", "intField", TYPE.NUMERICAL, false);
    assertEquals(1, matrix2.columnSize());
    SolrFieldMatrix matrix3 = new SolrFieldMatrix(solrServer, "id", "boolField", TYPE.BOOLEAN, false);
    assertEquals(1, matrix3.columnSize());
    SolrFieldMatrix matrix4 = new SolrFieldMatrix(solrServer, "id", "naryStringField", TYPE.MULTINOMIAL, false);
    assertEquals(3, matrix4.columnSize());
    SolrFieldMatrix matrix5 = new SolrFieldMatrix(solrServer, "id", "multiNaryStringField", TYPE.MULTINOMIAL, false);
    assertEquals(3, matrix5.columnSize());
  }


  @Test
  public void testGet() throws Exception {
    SolrFieldMatrix matrix = new SolrFieldMatrix(solrServer, "id", "boolField", TYPE.BOOLEAN, false);
    assertEquals(1.0, matrix.get(1, 0), 0.0);
    assertEquals(0.0, matrix.get(2, 0), 0.0);
    assertEquals(1.0, matrix.get(3, 0), 0.0);

    SolrFieldMatrix matrix5 = new SolrFieldMatrix(solrServer, "id", "intField", TYPE.NUMERICAL, false);
    assertEquals(10, matrix5.get(1, 0), 0);
    assertEquals(20, matrix5.get(2, 0), 0);
    assertEquals(30, matrix5.get(3, 0), 0);

    SolrFieldMatrix matrix2 = new SolrFieldMatrix(solrServer, "id", "textField", TYPE.TEXT, false);
    assertEquals(0.33, matrix2.get(1, matrix2.getColumnLabelBindings().get("Sentence")), 0.01);
    assertEquals(1, matrix2.get(1, matrix2.getColumnLabelBindings().get("one")), 0.00);
    assertEquals(0, matrix2.get(1, matrix2.getColumnLabelBindings().get("two")), 0.00);

    assertEquals(0.33, matrix2.get(1, matrix2.getColumnLabelBindings().get("Sentence")), 0.01);
    assertEquals(1, matrix2.get(2, matrix2.getColumnLabelBindings().get("two")), 0.00);
    assertEquals(0, matrix2.get(2, matrix2.getColumnLabelBindings().get("three")), 0.00);

    assertEquals(0.33, matrix2.get(1, matrix2.getColumnLabelBindings().get("Sentence")), 0.01);
    assertEquals(1, matrix2.get(3, matrix2.getColumnLabelBindings().get("three")), 0.00);
    assertEquals(0, matrix2.get(3, matrix2.getColumnLabelBindings().get("one")), 0.00);

    SolrFieldMatrix matrix3 = new SolrFieldMatrix(solrServer, "id", "naryStringField", TYPE.MULTINOMIAL, false);
    assertEquals(1, matrix3.get(1, matrix3.getColumnLabelBindings().get("cat1")), 0);
    assertEquals(0, matrix3.get(1, matrix3.getColumnLabelBindings().get("cat2")), 0);
    assertEquals(1, matrix3.get(2, matrix3.getColumnLabelBindings().get("cat2")), 0);
    assertEquals(0, matrix3.get(2, matrix3.getColumnLabelBindings().get("cat1")), 0);
    assertEquals(1, matrix3.get(3, matrix3.getColumnLabelBindings().get("cat3")), 0);
    assertEquals(0, matrix3.get(3, matrix3.getColumnLabelBindings().get("cat2")), 0);

    SolrFieldMatrix matrix4 = new SolrFieldMatrix(solrServer, "id", "multiNaryStringField", TYPE.MULTINOMIAL, true);
    assertEquals(1, matrix4.get(1, matrix3.getColumnLabelBindings().get("cat1")), 0);
    assertEquals(1, matrix4.get(1, matrix3.getColumnLabelBindings().get("cat2")), 0);
    assertEquals(1, matrix4.get(2, matrix3.getColumnLabelBindings().get("cat2")), 0);
    assertEquals(1, matrix4.get(2, matrix3.getColumnLabelBindings().get("cat3")), 0);
    assertEquals(1, matrix4.get(3, matrix3.getColumnLabelBindings().get("cat3")), 0);
    assertEquals(1, matrix4.get(3, matrix3.getColumnLabelBindings().get("cat1")), 0);
    assertEquals(0, matrix4.get(3, matrix3.getColumnLabelBindings().get("cat2")), 0);

  }

//  @Test
  public void testMostSimilars() throws  Exception {
    SolrFieldMatrix matrix = new SolrFieldMatrix(solrServer, "id", "textField", TYPE.TEXT, false);
    assertTrue(matrix.mostSimilars(3,1).contains(4));
  }

  @Test
  public void dummyTest() throws IOException, SolrServerException {
    assertEquals(1,1);
  }

  @After
  public void cleanup()throws IOException, SolrServerException{
    solrServer.deleteByQuery("*:*");
    solrServer.commit();
    container.shutdown();
    FileUtils.deleteDirectory(new File("src/test/resources/solr-home/fieldmatrix/data"));
  }
}
