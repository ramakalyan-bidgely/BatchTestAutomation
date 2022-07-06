package com.batch.utils;

import com.sun.jersey.api.client.*;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MultivaluedMap;
import java.io.*;
import java.util.List;
import java.util.Map;

public class RestLoggerFilter extends ClientFilter {
    private static final String NOTIFICATION_PREFIX = "* ";
    private static final String REQUEST_PREFIX = "> ";
    private static final String RESPONSE_PREFIX = "< ";
    private Logger logger = LoggerFactory.getLogger(getClass());
    private final int maxEntitySize;
    private long _id = 0;

    private final class Adapter extends AbstractClientRequestAdapter {
        private final StringBuilder b;

        Adapter(ClientRequestAdapter cra, StringBuilder b) {
            super(cra);
            this.b = b;
        }

        @Override
        public OutputStream adapt(ClientRequest request, OutputStream out) throws IOException {
            return new LoggingOutputStream(getAdapter().adapt(request, out), b);
        }

    }

    protected final class LoggingOutputStream extends OutputStream {
        private final OutputStream out;

        private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        private final StringBuilder b;

        LoggingOutputStream(OutputStream out, StringBuilder b) {
            this.out = out;
            this.b = b;
        }

        @Override
        public void write(byte[] b) throws IOException {
            baos.write(b);
            out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            baos.write(b, off, len);
            out.write(b, off, len);
        }

        @Override
        public void write(int b) throws IOException {
            baos.write(b);
            out.write(b);
        }

        @Override
        public void close() throws IOException {
            printEntity(b, baos.toByteArray());
            log(b);
            out.close();
        }
    }

    public RestLoggerFilter() {
        this(10 * 1024);
    }

    public RestLoggerFilter(int maxEntitySize) {
        this.maxEntitySize = maxEntitySize;
    }

    public RestLoggerFilter(Logger logger, int maxEntitySize) {
        this.logger = logger;
        this.maxEntitySize = maxEntitySize;
    }

    private void log(StringBuilder b) {
        if (logger.isDebugEnabled()) {
            logger.debug(b.toString());
        }
    }

    private StringBuilder prefixId(StringBuilder b, long id) {
        b.append(Long.toString(id)).append(" ");
        return b;
    }

    @Override
    public ClientResponse handle(ClientRequest request) throws ClientHandlerException {
        final long id = ++_id;

        logRequest(id, request);

        final ClientResponse response = getNext().handle(request);

        logResponse(id, response);

        return response;
    }

    private void logRequest(long id, ClientRequest request) {
        final StringBuilder b = new StringBuilder();

        printRequestLine(b, id, request);
        printRequestHeaders(b, id, request.getHeaders());

        if (request.getEntity() != null) {
            request.setAdapter(new Adapter(request.getAdapter(), b));
        } else {
            log(b);
        }
    }

    private void printRequestLine(StringBuilder b, long id, ClientRequest request) {
        prefixId(b, id).append(NOTIFICATION_PREFIX).append("Client out-bound request").append("\n");
        prefixId(b, id).append(REQUEST_PREFIX).append(request.getMethod()).append(" ")
                .append(request.getURI().toASCIIString()).append("\n");
    }

    private void printRequestHeaders(StringBuilder b, long id, MultivaluedMap<String, Object> headers) {
        for (final Map.Entry<String, List<Object>> e : headers.entrySet()) {
            final List<Object> val = e.getValue();
            final String header = e.getKey();

            if (val.size() == 1) {
                prefixId(b, id).append(REQUEST_PREFIX).append(header).append(": ")
                        .append(ClientRequest.getHeaderValue(val.get(0))).append("\n");
            } else {
                final StringBuilder sb = new StringBuilder();
                boolean add = false;
                for (final Object o : val) {
                    if (add) {
                        sb.append(',');
                    }
                    add = true;
                    sb.append(ClientRequest.getHeaderValue(o));
                }
                prefixId(b, id).append(REQUEST_PREFIX).append(header).append(": ").append(sb.toString()).append("\n");
            }
        }
    }

    private void logResponse(long id, ClientResponse response) {
        final StringBuilder b = new StringBuilder();

        printResponseLine(b, id, response);
        printResponseHeaders(b, id, response.getHeaders());

        InputStream stream = response.getEntityInputStream();
        try {
            if (!response.getEntityInputStream().markSupported()) {
                stream = new BufferedInputStream(stream);
                response.setEntityInputStream(stream);
            }

            stream.mark(maxEntitySize + 1);
            final byte[] entity = new byte[maxEntitySize + 1];
            final int entitySize = stream.read(entity);

            if (entitySize > 0) {
                b.append(new String(entity, 0, Math.min(entitySize, maxEntitySize)));
                if (entitySize > maxEntitySize) {
                    b.append("...more...");
                }
                b.append('\n');
                stream.reset();
            }
        } catch (final IOException ex) {
            throw new ClientHandlerException(ex);
        }
        log(b);
    }

    private void printResponseLine(StringBuilder b, long id, ClientResponse response) {
        prefixId(b, id).append(NOTIFICATION_PREFIX).append("Client in-bound response").append("\n");
        prefixId(b, id).append(RESPONSE_PREFIX).append(Integer.toString(response.getStatus())).append("\n");
    }

    private void printResponseHeaders(StringBuilder b, long id, MultivaluedMap<String, String> headers) {
        for (final Map.Entry<String, List<String>> e : headers.entrySet()) {
            final String header = e.getKey();
            for (final String value : e.getValue()) {
                prefixId(b, id).append(RESPONSE_PREFIX).append(header).append(": ").append(value).append("\n");
            }
        }
        prefixId(b, id).append(RESPONSE_PREFIX).append("\n");
    }

    private void printEntity(StringBuilder b, byte[] entity) throws IOException {
        if (entity.length == 0) {
            return;
        }
        b.append(new String(entity)).append("\n");
    }
}
