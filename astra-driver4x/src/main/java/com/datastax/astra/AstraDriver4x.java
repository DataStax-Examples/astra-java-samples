package com.datastax.astra;

import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;


/**
 * This class shows how to connect to the Astra (Cassandra as a service by DataStax)
 * 
 * https://astra.datastax.com
 */
public class AstraDriver4x {
    
    static final String ASTRA_ZIP_FILE = "<path_to_secureConnectBundle.zip>";
    static final String ASTRA_USERNAME = "<provide_a_clientId>";
    static final String ASTRA_PASSWORD = "<provide_a_clientSecret>";
    static final String ASTRA_KEYSPACE = "<provide_your_keyspace>";
    
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(AstraDriver4x.class);
        // Connect
        try (CqlSession cqlSession = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(ASTRA_ZIP_FILE))
                .withAuthCredentials(ASTRA_USERNAME, ASTRA_PASSWORD)
                .withKeyspace(ASTRA_KEYSPACE)
                .build()) {
            logger.info("[OK] Welcome to ASTRA. Connected to Keyspace {}", cqlSession.getKeyspace().get());
        }
        logger.info("[OK] Success");
        System.exit(0);
    }

}
