package com.batch.utils;

import com.google.common.base.Stopwatch;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class RestPerfFilter extends ClientFilter {
    private Logger logger = LoggerFactory.getLogger(getClass());

    public RestPerfFilter() {
    }

    public RestPerfFilter(Logger logger) {
        this.logger = logger;
    }

    @Override
    public ClientResponse handle(ClientRequest cr) throws ClientHandlerException {
        final Stopwatch sw = Stopwatch.createStarted();
        ClientResponse response = null;
        try {
            response = getNext().handle(cr);
        } finally {
            final String status = (response != null) ? String.valueOf(response.getStatus()) : "Unknown";
            if (logger.isDebugEnabled()) {
                logger.debug("Method:{}  Status:{}  URI:{}  TimeTaken(ms): {}", cr.getMethod(), status, cr.getURI(),
                        sw.elapsed(TimeUnit.MILLISECONDS));
            }
        }
        return response;
    }

}
