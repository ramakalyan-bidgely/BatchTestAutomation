package com.batch.utils;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestRetryFilter extends ClientFilter {
    private final int maxRetries;
    private final long delay;
    private final Logger logger = LoggerFactory.getLogger(RestRetryFilter.class);

    public RestRetryFilter(int maxRetries) {
        this.maxRetries = maxRetries;
        delay = 2000l;
    }

    public RestRetryFilter(int maxRetries, long delay) {
        this.maxRetries = maxRetries;
        this.delay = delay;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    @Override
    public ClientResponse handle(ClientRequest cr) throws ClientHandlerException {
        int i = 0;
        Throwable exception = null;
        String serverResponse = "";
        do {
            i++;
            logger.debug("Attempt {} URI: {}", i, cr.getURI());
            ClientResponse response = null;
            try {
                response = getNext().handle(cr);
                if (response.getStatus() / 100 != 5) {
                    return response;
                } else {
                    serverResponse = response.getEntity(String.class);
                    logger.error("Rest call attempt {} failed. URI: {} StatusCode: {} ErrorMessage: {}", i, cr.getURI(),
                            response.getStatus(), serverResponse);
                    try {
                        Thread.sleep(delay);
                    } catch (final InterruptedException e) {
                        logger.error("Error while trying to sleep", e);
                        throw new RuntimeException(e);
                    }
                }
            } catch (final ClientHandlerException e) {
                if (response != null) {
                    response.close();
                }
                logger.error("Rest call attempt {} failed.", i, e);
                exception = e;
                try {
                    Thread.sleep(delay);
                } catch (final InterruptedException e1) {
                    logger.error("Error while trying to sleep", e1);
                    throw new RuntimeException(e1);
                }
            }
        } while (i < maxRetries);

        if (exception != null) {
            throw new ClientHandlerException("Connection retries limit exceeded. Nested ClientHandlerException",
                    exception);
        } else {
            throw new ClientHandlerException("Connection retries limit exceeded. Client response: " + serverResponse);
        }
    }
}
