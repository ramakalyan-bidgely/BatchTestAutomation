package com.batch.utils;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.client.apache4.ApacheHttpClient4Handler;
import com.sun.jersey.client.apache4.config.DefaultApacheHttpClient4Config;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.net.URI;

/**
 * Wrapper class for Jersey client. It can be used to create
 * @author $author$
 * @version $Revision$, $Date$ *
 */
public class RestClient {
    private static final Logger logger = LoggerFactory.getLogger(RestClient.class);
    public static final RestClient REST_CLIENT = new RestClient();
    public static final RestClient REST_CLIENT_WITH_RETRY_HANDLER = new RestClient(true);
    public static final RestClient REST_CLIENT_WITH_TIMEOUT = new RestClient(true, 5000, 5000);
    private static final int MAX_RETRIES = 3;
    private final Client client;

    private RestClient() {
        client = config(false);
    }

    private RestClient(final boolean withRetryHandler) {
        client = config(withRetryHandler);
    }

    private RestClient(final boolean withRetryHandler, final int socketTimeout, final int connectionTimeout) {
        client = config(withRetryHandler, socketTimeout, connectionTimeout);
    }

    public WebResource resource(final URI uri) {
        return client.resource(uri);
    }

    public Client getClient() {
        return client;
    }

    private Client config(final boolean withRetryHandler) {
        int connectionTimeOut = 0;

        if (System.getProperty("connectionTimeOut") != null) {
            connectionTimeOut = Integer.valueOf(System.getProperty("connectionTimeOut"));
            logger.info("Setting connectionTimeOut value: {}", connectionTimeOut);
        }

        int socketTimeOut = 0;

        if (System.getProperty("socketTimeOut") != null) {
            socketTimeOut = Integer.valueOf(System.getProperty("socketTimeOut"));
            logger.info("Setting socketTimeOut value: {}", socketTimeOut);
        }
        return config(withRetryHandler, socketTimeOut, connectionTimeOut);
    }

    private Client config(final boolean withRetryHandler, final int socketTimeOut, final int connectionTimeOut) {
        final int maxPerRoute = 50;
        final int maxTotal = 50;
        logger.info("Initializing RestClient with ConnectionPool maxConnections: {} maxPerRoute: {}", maxTotal,
                maxPerRoute);
        final SSLContext sslContext = SSLContexts.createSystemDefault();
        final SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext,
                NoopHostnameVerifier.INSTANCE);
        final Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder
                .<ConnectionSocketFactory>create().register("https", sslsf)
                .register("http", new PlainConnectionSocketFactory()).build();
        final PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager(
                socketFactoryRegistry);
        connManager.setDefaultMaxPerRoute(maxPerRoute);
        connManager.setMaxTotal(maxTotal);

        final RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(connectionTimeOut)
                .setSocketTimeout(socketTimeOut).setCookieSpec(CookieSpecs.IGNORE_COOKIES).build();
        final CloseableHttpClient httpClient = HttpClientBuilder.create().setConnectionManager(connManager)
                .setDefaultRequestConfig(requestConfig).build();

        final ApacheHttpClient4Handler clientHandler = new ApacheHttpClient4Handler(httpClient, null, false);
        final ClientConfig config = new DefaultApacheHttpClient4Config();
        config.getFeatures().put("com.sun.jersey.api.json.POJOMappingFeature", true);
        config.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);
        config.getClasses().add(JacksonJsonProvider.class);
        final Client clientObject = new Client(clientHandler, config);
        clientObject.addFilter(new RestPerfFilter());
        clientObject.addFilter(new RestLoggerFilter());
        if (withRetryHandler) {
            clientObject.addFilter(new RestRetryFilter(MAX_RETRIES));
        }
        return clientObject;
    }

}
