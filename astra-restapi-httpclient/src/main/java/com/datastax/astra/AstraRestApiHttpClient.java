package com.datastax.astra;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class shows how to connect to the REST API.
 * 
 * https://astra.datastax.com
 */
public class AstraRestApiHttpClient {
    
    static final String ASTRA_TOKEN       = "<change_with_your_token>";
    static final String ASTRA_DB_ID       = "<change_with_your_database_identifier>";
    static final String ASTRA_DB_REGION   = "<change_with_your_database_region>";
    static final String ASTRA_DB_KEYSPACE = "<change_with_your_keyspace>";
    
    static  Logger logger = LoggerFactory.getLogger(AstraRestApiHttpClient.class);
    
    public static void main(String[] args) throws Exception {
        
        String apiRestEndpoint = new StringBuilder("https://")
                .append(ASTRA_DB_ID).append("-")
                .append(ASTRA_DB_REGION)
                .append(".apps.astra.datastax.com/api/rest")
                .toString();
        logger.info("Rest Endpoint is {}", apiRestEndpoint);
        
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            listKeyspaces(httpClient, apiRestEndpoint);
            createTable(httpClient, apiRestEndpoint);
            insertRow(httpClient, apiRestEndpoint);
            retrieveRow(httpClient, apiRestEndpoint);
        }
        logger.info("[OK] Success");
        System.exit(0);
    }
    
    /**
     * List Keyspaces.
     * 
     * @param httpClient
     *      current client
     * @param apiRestEndpoint
     *      api rest endpoint
     * @throws Exception 
     *      parsing or access errors
     */
    private static void listKeyspaces(CloseableHttpClient httpClient, String apiRestEndpoint) throws Exception {
        // Build Request
        HttpGet listKeyspacesReq = new HttpGet(apiRestEndpoint + "/v2/schemas/keyspaces");
        listKeyspacesReq.addHeader("X-Cassandra-Token", ASTRA_TOKEN);
        // Execute Request
        try(CloseableHttpResponse res = httpClient.execute(listKeyspacesReq)) {
            if (200 == res.getCode()) {
                logger.info("[OK] Keyspaces list retrieved");
                logger.info("Returned message: {}", EntityUtils.toString(res.getEntity()));
                
            }
        }
    }
    
    /**
     * Create table users.
     * 
     * @param httpClient
     *      current client
     * @param apiRestEndpoint
     *      api rest endpoint
     * @throws Exception 
     *      parsing or access errors
     */
    private static void createTable(CloseableHttpClient httpClient, String apiRestEndpoint) throws Exception {
        HttpPost createTableReq = new HttpPost(apiRestEndpoint + "/v2/schemas/keyspaces/" + ASTRA_DB_KEYSPACE + "/tables");
        createTableReq.addHeader("X-Cassandra-Token", ASTRA_TOKEN);
        String createTableRequest = "{\n"
                + "  \"name\": \"users\",\n"
                + "  \"columnDefinitions\":\n"
                + "    [\n"
                + "        {\n"
                + "        \"name\": \"firstname\",\n"
                + "        \"typeDefinition\": \"text\"\n"
                + "      },\n"
                + "        {\n"
                + "        \"name\": \"lastname\",\n"
                + "        \"typeDefinition\": \"text\"\n"
                + "      },\n"
                + "      {\n"
                + "        \"name\": \"email\",\n"
                + "        \"typeDefinition\": \"text\"\n"
                + "      },\n"
                + "        {\n"
                + "        \"name\": \"color\",\n"
                + "        \"typeDefinition\": \"text\"\n"
                + "      }\n"
                + "    ],\n"
                + "  \"primaryKey\":\n"
                + "    {\n"
                + "      \"partitionKey\": [\"firstname\"],\n"
                + "      \"clusteringKey\": [\"lastname\"]\n"
                + "    },\n"
                + "  \"ifNotExists\": true,\n"
                + "  \"tableOptions\":\n"
                + "    {\n"
                + "      \"defaultTimeToLive\": 0,\n"
                + "      \"clusteringExpression\":\n"
                + "        [{ \"column\": \"lastname\", \"order\": \"ASC\" }]\n"
                + "    }\n"
                + "}";
        createTableReq.setEntity(new StringEntity(createTableRequest, ContentType.APPLICATION_JSON));
        // Execute Request
        try(CloseableHttpResponse res = httpClient.execute(createTableReq)) {
            if (201 == res.getCode()) {
                logger.info("[OK] Table Created (if needed)");
                logger.info("Returned message: {}", EntityUtils.toString(res.getEntity()));
            }
        }
    }
    
    /**
     * Insert a Row
     * 
     * @param httpClient
     *      current client
     * @param apiRestEndpoint
     *      api rest endpoint
     * @throws Exception 
     *      parsing or access errors
     */
    private static void insertRow(CloseableHttpClient httpClient, String apiRestEndpoint) throws Exception {
        HttpPost insertCedrick = new HttpPost(apiRestEndpoint + "/v2/keyspaces/" + ASTRA_DB_KEYSPACE + "/users" );
        insertCedrick.addHeader("X-Cassandra-Token", ASTRA_TOKEN);
        insertCedrick.setEntity(new StringEntity("{"
                + " \"firstname\": \"Cedrick\","
                + " \"lastname\" : \"Lunven\","
                + " \"email\"    : \"c.lunven@gmail.com\","
                + " \"color\"    : \"blue\" }", ContentType.APPLICATION_JSON));
        
        // Execute Request
        try(CloseableHttpResponse res = httpClient.execute(insertCedrick)) {
            if (201 == res.getCode()) {
                logger.info("[OK] Row inserted");
                logger.info("Returned message: {}", EntityUtils.toString(res.getEntity()));
            }
        }
    }
    
    /**
     * Insert a Row
     * 
     * @param httpClient
     *      current client
     * @param apiRestEndpoint
     *      api rest endpoint
     * @throws Exception 
     *      parsing or access errors
     */
    private static void retrieveRow(CloseableHttpClient httpClient, String apiRestEndpoint) throws Exception {

        // Build Request
        HttpGet rowReq = new HttpGet(apiRestEndpoint + "/v2/keyspaces/" + ASTRA_DB_KEYSPACE + "/users/Cedrick/Lunven" );
        rowReq.addHeader("X-Cassandra-Token", ASTRA_TOKEN);
        
        // Execute Request
        try(CloseableHttpResponse res = httpClient.execute(rowReq)) {
            if (200 == res.getCode()) {
                String payload =  EntityUtils.toString(res.getEntity());
                logger.info("[OK] Row retrived");
                logger.info("Row retrieved : {}", payload);
            }
        }
    }
}
