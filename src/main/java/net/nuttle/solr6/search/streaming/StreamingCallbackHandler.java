package net.nuttle.solr6.search.streaming;

import java.util.concurrent.BlockingQueue;

import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.common.SolrDocument;

public class StreamingCallbackHandler extends StreamingResponseCallback 
{
  BlockingQueue<SolrDocument> queue;
  int count = 0;
  long expectedCount = -1;
  long numFound = 0;

  public StreamingCallbackHandler(BlockingQueue<SolrDocument> queue) {
    this.queue = queue;
  }

  /**
   * Specify value for rows if it is intended to stream a number of documents
   * less than the total number of matches.
   * @param queue
   * @param rows
   */
  public StreamingCallbackHandler(BlockingQueue<SolrDocument> queue, int rows) {
    if (expectedCount < 0) {
      throw new IllegalArgumentException("rows argument must be >= 0");
    }
    this.queue = queue;
    this.expectedCount = rows;
  }
  
  @Override
  public void streamSolrDocument(SolrDocument doc) {
    queue.add(doc);
    count++;
    if (expectedCount >= 0) {
      if (count == expectedCount || count == numFound) {
        queue.add(new MarkerDocument());
      }
    } else {
      if (count == numFound) {
        queue.add(new MarkerDocument());
      }
    }
  }
  
  @Override
  public void streamDocListInfo(long numFound, long start, Float maxScore) {
    this.numFound = numFound;
    if (numFound == 0) {
      queue.add(new MarkerDocument());
    }
  }
  
  public static class MarkerDocument extends SolrDocument {}
}
