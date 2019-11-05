package com.datastax.examples;

import com.datastax.driver.core.*;
import java.io.File;
import java.util.Set;
import java.util.stream.Collectors;

public class App {
    public static void main(String[] args)
    {
        //Get the client configuration based on the Environment variables passed in
        Session session = getClientConfiguration();

        Set<Host> hostSet = session.getCluster().getMetadata().getAllHosts();
        String hosts = hostSet.stream().map(h -> h.toString()).collect(Collectors.joining(","));
        System.out.printf("Connected to cluster with %d host(s) %s\n", hostSet.size(), hosts);

        // This step is important because it frees underlying resources (Threads, Connections, etc) and needs to be done at shutdown
        session.close();
        System.exit(0);
    }

    private static Session getClientConfiguration() {
        if (System.getenv("USEAPOLLO") != null && Boolean.parseBoolean(System.getenv("USEAPOLLO"))) {
            if (System.getenv("DBUSERNAME") != null &&
                    System.getenv("DBPASSWORD") != null &&
                    System.getenv("SECURECONNECTBUNDLEPATH") != null &&
                    System.getenv("KEYSPACE") != null) {
                return Cluster.builder()
                        // Change the path here to the secure connect bundle location
                        .withCloudSecureConnectBundle(new File(System.getenv("SECURECONNECTBUNDLEPATH")))
                        // Set the user_name and password here for the Apollo instance
                        //Apollo uses a PlainTextAuthProvider for the credentials
                        .withAuthProvider(new PlainTextAuthProvider(System.getenv("DBUSERNAME"), System.getenv("DBPASSWORD")))
                        //Now build the cluster and connect to the keyspace
                        .build().connect(System.getenv("KEYSPACE"));
            } else {
                throw new IllegalArgumentException("You must have the DBUSERNAME, DBPASSWORD, SECURECONNECTBUNDLEPATH, and KEYSPACE environment variables set to use Apollo as your database.");
            }
        } else {
            Cluster.Builder builder = Cluster.builder();
            if (System.getenv("CONTACTPOINTS") != null) {
                builder.addContactPoint(System.getenv("CONTACTPOINTS"));

                //If authentication credentials were specified then use them
                if (System.getenv("DBUSERNAME") != null && System.getenv("DBPASSWORD") != null) {
                    builder.withAuthProvider(new PlainTextAuthProvider(System.getenv("DBUSERNAME"), System.getenv("DBPASSWORD")));
                }

                //If a keyspace is specified then use it
                if (System.getenv("KEYSPACE") != null) {
                    return builder.build().connect(System.getenv("DBUSERNAME"));
                } else {
                    return builder.build().connect();
                }

            } else {
                throw new IllegalArgumentException("You must have the CONTACTPOINTS environment variables set to use DSE/DDAC/Cassandra as your database.");
            }
        }
    }
}
