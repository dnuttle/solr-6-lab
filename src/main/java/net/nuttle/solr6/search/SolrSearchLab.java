package net.nuttle.solr6.search;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.request.schema.AnalyzerDefinition;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest.DefaultQueryOperator;
import org.apache.solr.client.solrj.request.schema.SchemaRequest.FieldTypes;
import org.apache.solr.client.solrj.request.schema.SchemaRequest.SchemaName;
import org.apache.solr.client.solrj.request.schema.SchemaRequest.SchemaVersion;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.schema.FieldTypeRepresentation;
import org.apache.solr.client.solrj.response.schema.SchemaResponse.DefaultQueryOperatorResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse.FieldTypesResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse.SchemaNameResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse.SchemaVersionResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

import net.nuttle.solr6.search.streaming.StreamingCallbackHandler;
import net.nuttle.solr6.search.streaming.StreamingCallbackHandler.MarkerDocument;

public class SolrSearchLab {
  
  private static final String COL1 = "col1";
  private static final String HOST = "http://localhost:8983/solr";
  private static final String COL1_HOST = HOST + "/" + COL1;
  
  private static long numDocsFound;
  private static long startDoc;
  private static Float maxDocScore;

  /*
   * TO-DO's:
   * Look at SolrClient.queryAndStreamResponse
   */
  
  public static void main(String[] args) throws Exception {
    SolrClient client = getSolrClient(COL1_HOST);
    queryAndStreamResponse(client);
    queryAndStreamResponse2(client);
    //queryRequest(client);
    //lowLevelSearch(client);
    //pingRequest();
    //lowLevelSearch();
    //simpleSearch();
    client.close();
  }
  
  /**
   * queryAndStreamResponse does not block.  So you need some kind of mechanism to keep 
   * reading from whatever you put the documents into.  Here, I'm using a SolrDocumentList,
   * which is not real-world.  Instead you want something like a queue, and better yet, a blocking queue,
   * so that you keep trying to read from it, and it blocks whenever it's empty, then proceeds when it
   * gets more streamed results.  But this means that, when the results really are finished, you
   * put a special "marker" document into the queue.
   * @param client
   * @throws Exception
   */
  private static void queryAndStreamResponse(SolrClient client) throws Exception {
    NamedList<String> list = new NamedList<>();
    list.add("q", "*:*");
    SolrParams params = SolrParams.toSolrParams(list);
    SolrDocumentList docList = new SolrDocumentList();
    client.queryAndStreamResponse(params, new StreamingResponseCallback() {
      @Override
      public void streamSolrDocument(SolrDocument doc) {
        docList.add(doc);
      }
      @Override
      public void streamDocListInfo(long numFound, long start, Float maxScore) {
        numDocsFound = numFound;
        startDoc = start;
        maxDocScore = maxScore;
      }
    });
    System.out.println("Number of hits: " + numDocsFound);
    System.out.println("Docs returned: " + docList.size());
  }
  
  /**
   * An example of a streaming request using a BlockingQueue
   * @throws Exception
   */
  private static void queryAndStreamResponse2(SolrClient client) throws Exception {
    NamedList<String> list = new NamedList<>();
    list.add("q", "*:*");
    //Set rows to huge number to ensure that we return all matching documents
    list.add("rows", "" + Integer.MAX_VALUE);
    SolrParams params = SolrParams.toSolrParams(list);
    SolrDocumentList docList = new SolrDocumentList();
    //No-arg constructor for LinkedBlockingQueue.  If an int was passed,
    //this would be maximum capacity for queue, and we don't want that; although
    //we don't expect a huge number of items in the queue at any given time either.
    //If a capacity was set, and queue already contained that many items,
    //it would block adding more until capacity was available.
    //Note that ArrayBlockingQueue is another implementation, but all of its
    //constructors require that capacity value be set.  So LinkedBlockingQueue it is.
    //If we wanted to limit the number of documents to a specific number, and we were 
    //confident that there would be at least that many matches, we might use
    //ArrayBlockingQueue, but this would still set aside memory for the entire number
    //at once, so if the number is large, LinkedBlockingQueue is probably still better.
    //However this means that items in the queue should be pulled out about as fast as they 
    //are put in, or again, runaway memory usage.  If the items are taken out in batches
    //and processed in calls that can themselves be blocked, such as http calls, this 
    //can turn into a logjam.
    //Our use case is to get product ids and write them straight to a file.  There is little
    //concern that this will be blocked, unless HDFS becomes unavailable.
    BlockingQueue<SolrDocument> queue = new LinkedBlockingQueue<>();
    StreamingCallbackHandler handler = new StreamingCallbackHandler(queue);
    client.queryAndStreamResponse(params, handler);
    SolrDocument doc;
    int count = 0;
    /*
     * In real-world example, each doc would be processed somehow.  For example, in our data-nulling code,
     * get the id field and put it in a list.  Here we just keep a count.
     */
    do {
      doc = queue.take();
      if (!(doc instanceof MarkerDocument)) {
        count++;
      }
    } while (!(doc instanceof MarkerDocument));
    System.out.println("Matches found: " + count);
  }
  
  /**
   * An example of a search request using low-level objects.  This skips the abstraction and
   * convenience of using SolrQuery and getting a QueryResponse
   * Subclasses of SolrRequest include:
   * AbstractSchemaRequest
   * AbstractUpdateRequest
   * CollectionAdminRequest
   * ConfigSetAdminRequest
   * CoreAdminRequest
   * DirectXmlRequest
   * DocumentAnalysisRequest
   * FieldAnalysisRequest
   * GenericSolrRequest
   * LukeRequest
   * QueryRequest
   * SolrPing
   * @throws Exception
   */
  private static void lowLevelSearch(SolrClient client) throws Exception {
    NamedList<String> list = new NamedList<>();
    list.add("q", "*:*");
    SolrParams params = SolrParams.toSolrParams(list);
    SolrRequest<QueryResponse> req = new QueryRequest(params);
    /*
     * Note that if you call req.process(client) you get a QueryResponse object back
     */
    NamedList<Object> r = client.request(req);
    /*
     * Or you can just create your own QueryResponse object using the NamedList and SolrClient.
     * The SolrClient is used only for its internal DocumentObjectBinder object.
     */
    QueryResponse resp = new QueryResponse(r, client);
    /*
     * You could also do this:
     * QueryResponse resp = new QueryResponse();
     * resp.setResponse(r);
     * In the above case, a default DocumentObjectBinder instance must be created, this might lead to errors(?)
     * Or this:
     * QueryResponse resp = new QueryResponse(client);
     * resp.setResponse(r);
     */
    System.out.println("Total matches: " + resp.getResults().getNumFound());
    for (int i = 0; i < r.size(); i++) {
      System.out.println(r.getName(i) + ":" + r.getVal(i) + " " + r.getVal(i).getClass().getName());
    }
    
    SolrDocumentList doclist = (SolrDocumentList) r.get("response");
    for (SolrDocument doc : doclist) {
      System.out.println("============================");
      for (String field : doc.getFieldNames()) {
        System.out.println(field + ":" + doc.getFieldValue(field));
      }
    }
    client.close();
  }

  private static void queryRequest(SolrClient client) throws Exception {
    NamedList<String> list = new NamedList<>();
    list.add("q", "*:*");
    SolrParams params = SolrParams.toSolrParams(list);
    QueryRequest req = new QueryRequest(params);
    QueryResponse resp = req.process(client);
    System.out.println("Total matches: " + resp.getResults().getNumFound());
  }
  
  /**
   * Sometimes you call SolrClient.request and pass in an instance of SolrRequest, but with SolrPing, that doesn't work.
   * Instead you call SolrClient.ping(), which creates an instance of SolrPing internally.
   * Or you can duplicate this by creating an instance of SolrPing and calling its process method.
   * @throws Exception
   */
  private static void pingRequest(SolrClient client) throws Exception {
    SolrPing req = new SolrPing();
    SolrPingResponse resp = req.process(client);
    System.out.println("Elapsed time: " + resp.getElapsedTime());
    //You can skip creating the SolrPing instance:
    resp = client.ping();
    System.out.println("Elapsed time: " + resp.getElapsedTime());
    client.close();
  }

  private static void schemaNameRequest(SolrClient client) throws Exception {
    SchemaRequest.SchemaName req = new SchemaName();
    SchemaNameResponse resp = req.process(client);
    System.out.println("Schema name: " + resp.getSchemaName());
    client.close();
  }
  
  private static void schemaVersionRequest(SolrClient client) throws Exception {
    SchemaRequest.SchemaVersion verReq = new SchemaVersion();
    SchemaVersionResponse verResp = verReq.process(client);
    System.out.println("Schema version: " + verResp.getSchemaVersion());
    client.close();
  }
  
  private static void defaultQueryOperatorRequest(SolrClient client) throws Exception {
    SchemaRequest.DefaultQueryOperator dqoReq = new DefaultQueryOperator();
    DefaultQueryOperatorResponse dpoResp = dqoReq.process(client);
    System.out.println("Default query operator: " + dpoResp.getDefaultOperator());
  }
  
  private static void fieldTypesRequest(SolrClient client) throws Exception {
    SchemaRequest.FieldTypes ftReq = new FieldTypes();
    FieldTypesResponse ftResp = ftReq.process(client);
    List<FieldTypeRepresentation> fts = ftResp.getFieldTypes();
    for (FieldTypeRepresentation ft : fts) {
      Map<String,Object> attrs = ft.getAttributes();
      System.out.println("FieldTypeRepresentation:");
      for (String key : attrs.keySet()) {
        System.out.println("  " + key + ":" + attrs.get(key).toString());
      }
      if (ft.getAnalyzer() != null) {
        System.out.println("  Analyzer: " + getAnalyzerInfo(ft.getAnalyzer()));
      }
      if (ft.getIndexAnalyzer() != null) {
        System.out.println("  Index analyzer: " + getAnalyzerInfo(ft.getIndexAnalyzer()));
      }
      if (ft.getQueryAnalyzer() != null) {
        System.out.println("  Query analyzer: " + getAnalyzerInfo(ft.getQueryAnalyzer()));
      }
      
    }
    client.close();
  }
  
  private static String getAnalyzerInfo(AnalyzerDefinition def) {
    StringBuilder sb = new StringBuilder();
    sb.append("Tokenizer: [");
    for (String key : def.getTokenizer().keySet()) {
      sb.append(key).append("=").append(def.getTokenizer().get(key));
    }
    sb.append("] ");
    if (def.getFilters() != null && def.getFilters().size() > 0) {
      sb.append(" Filters: [");
      String prefix = "";
      for (Map<String,Object> filter : def.getFilters()) {
        sb.append(prefix);
        prefix = ", ";
        sb.append("{");
        String prefix2 = "";
        for (String key : filter.keySet()) {
          sb.append(prefix2).append(key).append(":").append(filter.get(key).toString());
          prefix2 = ", ";
        }
        sb.append("}");
      }
    }
    if (def.getCharFilters() != null && def.getCharFilters().size() > 0) {
      sb.append(" CharFilters: [");
      String prefix = "";
      for (Map<String,Object> filter : def.getCharFilters()) {
        sb.append(prefix);
        prefix = ", ";
        sb.append("{");
        String prefix2 = "";
        for (String key : filter.keySet()) {
          sb.append(prefix2).append(key).append(":").append(filter.get(key).toString());
          prefix2 = ", ";
        }
        sb.append("}");
      }
      
    }
    return sb.toString();
  }
  
  private static void simpleSearch(SolrClient client) throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    QueryResponse response = client.query(q);
    System.out.println(response.getResults().size());
    SolrDocumentList docs = response.getResults();
    for (SolrDocument doc : docs) {
      System.out.println(doc.getFieldValue("id"));
      Map<String,Object> valMap = doc.getFieldValueMap();
      for (String key : valMap.keySet()) {
        if (doc.getFieldValues(key) != null) {
          System.out.print("    " + key + "=");
          String prefix = "";
          for (Object value : doc.getFieldValues(key)) {
            System.out.print(prefix);
            prefix = ", ";
            System.out.print("'" + value + "'");
          }
          System.out.println("");
        } else {
          System.out.println("   " + key + "=" + doc.getFieldValue(key));
        }
      }
    }
    client.close();
  }
  
  private static HttpSolrClient getSolrClient(String url) {
    Builder builder = new Builder(url);
    return builder.build();
  }
}
