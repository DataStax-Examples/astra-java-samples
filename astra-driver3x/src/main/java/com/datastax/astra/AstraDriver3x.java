package com.datastax.astra;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;


/**
 * This class shows how to connect to the Astra, Cassandra as a service by DataStax
 * 
 *  Pre-requisites:
 * ===================
 * 
 * 1. Create an Astra Account, a Database with a keyspace
 * 2. Download the secure connect bundle for this database
 * 3. Create a token to be used as credentials
 * 4. You need a java driver version 3.8+
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class AstraDriver3x {
    
    static final String ASTRA_ZIP_FILE = "<path_to_secureConnectBundle.zip>";
    static final String ASTRA_USERNAME = "<provide_a_clientId>";
    static final String ASTRA_PASSWORD = "<provide_a_clientSecret>";
    static final String ASTRA_KEYSPACE = "<provide_your_keyspace>";
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(AstraDriver3x.class);
        try(Cluster cluster = Cluster.builder()
                .withCloudSecureConnectBundle(new File(ASTRA_ZIP_FILE))
                .withCredentials(ASTRA_USERNAME, ASTRA_PASSWORD)
                .build() ) {
             Session session = cluster.connect(ASTRA_KEYSPACE);
             logger.info("[OK] Welcome to ASTRA. Connected to Keyspace {}", session.getLoggedKeyspace());
        }
        logger.info("[OK] Success");
        System.exit(0);
    }

}
