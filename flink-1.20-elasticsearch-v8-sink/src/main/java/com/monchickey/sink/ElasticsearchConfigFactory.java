package com.monchickey.sink;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.elasticsearch.client.RestClient;

import javax.net.ssl.SSLContext;
import java.io.Serializable;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import static org.apache.flink.shaded.curator5.com.google.common.base.Preconditions.checkNotNull;

public class ElasticsearchConfigFactory implements Serializable {

    private final HttpHost httpHost;

    private final String username;

    private final String password;

    public ElasticsearchConfigFactory(HttpHost httpHost, String username, String password) {
        this.httpHost = checkNotNull(httpHost);
        this.username = username;
        this.password = password;
    }

    public ElasticsearchClient create() throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

        SSLContext sslContext = SSLContextBuilder.create()
                .loadTrustMaterial(null, (chan, authType) -> true)
                .build();


        RestClient restClient = RestClient.builder(httpHost)
            .setHttpClientConfigCallback(httpClientBuilder ->
                (username != null && password != null) ?
                    httpClientBuilder.setSSLContext(sslContext).setDefaultCredentialsProvider(getCredentials())
                    : httpClientBuilder.setSSLContext(sslContext)
                ).build();

        return new ElasticsearchClient(new RestClientTransport(restClient, new JacksonJsonpMapper()));
    }

    private CredentialsProvider getCredentials() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY,
            new UsernamePasswordCredentials(username, password));

        return credentialsProvider;
    }
}
