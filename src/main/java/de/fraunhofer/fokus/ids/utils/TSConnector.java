package de.fraunhofer.fokus.ids.utils;

import io.vertx.core.AsyncResult;
import io.vertx.core.*;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.circuitbreaker.CircuitBreaker;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RDFParserBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class TSConnector {
    private WebClient client;
    private final Logger LOGGER = LoggerFactory.getLogger(TSConnector.class.getName());

    private String uri;
    private Map<String, String> values = new HashMap();

    private String username;
    private String password;
    private String dataEndpoint;
    private String queryEndpoint;

    private CircuitBreaker breaker;

    public static TSConnector create(WebClient client, CircuitBreaker breaker,JsonObject config) {
        return new TSConnector(client, breaker,config);
    }

    private TSConnector(WebClient client, CircuitBreaker breaker, JsonObject config) {
        this.client = client;
        this.breaker = breaker;
        this.uri = config.getString("VIRTUOSO_ADDRESS");
        this.username = config.getString("VIRTUOSO_USER");
        this.password = config.getString("VIRTUOSO_PASSWORD");
        this.dataEndpoint = config.getString("VIRTUOSO_DATAENDPOINT", "/sparql-graph-crud-auth");
        this.queryEndpoint = config.getString("VIRTUOSO_QUERYENDPOINT", "/sparql");
    }

    public static Lang mimeTypeToLang(String dataMimeType) {
        Lang lang = Lang.NTRIPLES;
        if (dataMimeType != null) {
            int idx = dataMimeType.indexOf(59);
            String type = idx != -1 ? dataMimeType.substring(0, idx) : dataMimeType;
            String var4 = type.trim();
            byte var5 = -1;
            switch(var4.hashCode()) {
                case -1417884249:
                    if (var4.equals("text/n3")) {
                        var5 = 4;
                    }
                    break;
                case -309045154:
                    if (var4.equals("text/turtle")) {
                        var5 = 3;
                    }
                    break;
                case -43840953:
                    if (var4.equals("application/json")) {
                        var5 = 2;
                    }
                    break;
                case -43544197:
                    if (var4.equals("application/trig")) {
                        var5 = 5;
                    }
                    break;
                case 886992732:
                    if (var4.equals("application/ld+json")) {
                        var5 = 1;
                    }
                    break;
                case 1894349687:
                    if (var4.equals("application/n-triples")) {
                        var5 = 6;
                    }
                    break;
                case 1969663169:
                    if (var4.equals("application/rdf+xml")) {
                        var5 = 0;
                    }
            }

            switch(var5) {
                case 0:
                    lang = Lang.RDFXML;
                    break;
                case 1:
                case 2:
                    lang = Lang.JSONLD;
                    break;
                case 3:
                    lang = Lang.TURTLE;
                    break;
                case 4:
                    lang = Lang.N3;
                    break;
                case 5:
                    lang = Lang.TRIG;
                    break;
                case 6:
                    lang = Lang.NTRIPLES;
            }
        }

        return lang;
    }

    public void getGraph(String graphName, Handler<AsyncResult<String>> handler) {
        HttpRequest<Buffer> request = client
                .getAbs(uri + dataEndpoint)
                .putHeader("Accept", "application/n-triples")
                .addQueryParam("graph", graphName);

        Promise<HttpResponse<Buffer>> responsePromise = Promise.promise();
        send(request, HttpMethod.GET, responsePromise);

        responsePromise.future().setHandler(ar -> {
            if (ar.succeeded()) {
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream()){
                    Model model = ModelFactory.createDefaultModel();
                    RDFParserBuilder builder = RDFParser.create().source(new ByteArrayInputStream(ar.result().body().getBytes())).lang((Lang)mimeTypeToLang("application/n-triples"));
                    builder.parse(model);
                    model.write(baos, "JSON-LD");
                    handler.handle(Future.succeededFuture(baos.toString()));
                } catch (Exception e) {
                    handler.handle(Future.failedFuture(e));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void deleteGraph(String graphName, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {
        HttpRequest<Buffer> request = client
                .deleteAbs(uri + dataEndpoint)
                .addQueryParam("graph", graphName);

        Promise<HttpResponse<Buffer>> responsePromise = Promise.promise();

        send(request, HttpMethod.DELETE, responsePromise);

        responsePromise.future().setHandler(ar -> {
            if (ar.succeeded()) {
                LOGGER.info("Delete graph succeeded : "+graphName);
                handler.handle(Future.succeededFuture());
            } else {
                LOGGER.info("Delete graph falied : "+graphName);
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void putGraph(String graph , Model model,Handler<AsyncResult<HttpResponse<Buffer>>> handler) {
        HttpRequest<Buffer> request = client
                .putAbs(uri + dataEndpoint)
                .putHeader("Content-Type", "application/n-triples")
                .addQueryParam("graph", graph);
        StringWriter writer = new StringWriter();
        RDFDataMgr.write(writer, model, Lang.NTRIPLES);
        String output = writer.toString();

        if (breaker != null) {
            breaker.<HttpResponse<Buffer>>execute(promise -> sendBuffer(request, HttpMethod.PUT, Buffer.buffer(output), promise))
                    .setHandler(ar -> {
                        if (ar.succeeded()) {
                            LOGGER.info("send buffer to viruoso succeeded");
                            handler.handle(Future.succeededFuture(ar.result()));
                        } else {
                            LOGGER.info("send buffer to viruoso failed "+ar.cause());
                            handler.handle(Future.failedFuture(ar.cause()));
                        }
                    });
        }else {
            Promise<HttpResponse<Buffer>> promise = Promise.promise();
            sendBuffer(request, HttpMethod.PUT, Buffer.buffer(output), promise);
            promise.future().setHandler(ar -> {
                if (ar.succeeded()) {
                    handler.handle(Future.succeededFuture(ar.result()));
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        }

    }

    private void sendBuffer(HttpRequest<Buffer> request, HttpMethod method, Buffer buffer, Promise<HttpResponse<Buffer>> promise) {
        request.sendBuffer(buffer, ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                if (response.statusCode() == 401) {
                    String authenticate = authenticate(response.getHeader("WWW-Authenticate"), uri, method.name(), username, password);
                    if (authenticate != null) {
                        request.putHeader("Authorization", authenticate);
                        sendBuffer(request, method, buffer, promise);
                    } else {
                        promise.fail("Could not authenticate");
                    }
                } else if (response.statusCode() == 200 || response.statusCode() == 201 || response.statusCode() == 204) {
                    promise.complete(response);
                } else {
                    promise.fail(response.statusCode() + " - " + response.statusMessage() + " - " + response.bodyAsString());
                }
            } else {
                promise.fail(ar.cause());
            }
        });
    }

    private void send(HttpRequest<Buffer> request, HttpMethod method, Promise<HttpResponse<Buffer>> promise) {
        request.send(ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                if (response.statusCode() == 401) {
                    String authenticate = authenticate(response.getHeader("WWW-Authenticate"), uri, method.name(), username, password);
                    if (authenticate != null) {
                        request.putHeader("Authorization", authenticate);
                        send(request, method, promise);
                    } else {
                        promise.fail("Could not authenticate");
                    }
                } else if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    promise.complete(response);
                } else {
                    promise.fail(new ReplyException(ReplyFailure.RECIPIENT_FAILURE, response.statusCode(), response.statusMessage()));
                }
            } else {
                promise.fail(ar.cause());
            }
        });
    }

    private String auth(String uri, String method, String username, String password) {
        if (this.values.isEmpty()) {
            return null;
        } else {
            long nc = 0L;
            String nonce = (String)this.values.get("nonce");
            String realm = (String)this.values.get("realm");
            byte[] cnonceBytes = new byte[8];
            (new Random()).nextBytes(cnonceBytes);
            String clientNonce = this.digest2HexString(cnonceBytes);
            String nonceCount = String.format("%08x", ++nc);
            DigestUtils a = new DigestUtils();
            String ha1 = (new DigestUtils()).md5Hex(username + ":" + realm + ":" + password);
            String ha2 = (new DigestUtils()).md5Hex(method + ":" + uri);
            String response = (new DigestUtils()).md5Hex(ha1 + ":" + nonce + ":" + nonceCount + ":" + clientNonce + ":auth:" + ha2);
            return "Digest username=\"" + username + "\", realm=\"" + realm + "\", nonce=\"" + nonce + "\", uri=\"" + uri + "\", cnonce=\"" + clientNonce + "\", nc=" + nonceCount + ", qop=auth, response=\"" + response + "\", algorithm=\"MD5\", state=true";
        }
    }
    private String digest2HexString(byte[] digest) {
        StringBuilder digestString = new StringBuilder();
        byte[] var3 = digest;
        int var4 = digest.length;

        for(int var5 = 0; var5 < var4; ++var5) {
            byte b = var3[var5];
            int low = b & 15;
            int hi = (b & 240) >> 4;
            digestString.append(Integer.toHexString(hi));
            digestString.append(Integer.toHexString(low));
        }

        return digestString.toString();
    }

    private void getValues(String wwwAuthenticate) {
        if (wwwAuthenticate.startsWith("Digest ")) {
            String[] pairs = wwwAuthenticate.substring(7).split(",");
            String[] var3 = pairs;
            int var4 = pairs.length;

            for(int var5 = 0; var5 < var4; ++var5) {
                String pair = var3[var5];
                String[] entry = pair.split("=");
                this.values.put(entry[0].trim(), entry[1].trim().replaceAll("\"", ""));
            }
        }
    }

    public  String authenticate(String wwwAuthenticate, String uri, String methodName, String username, String password) {
        getValues(wwwAuthenticate);
        return auth(uri,methodName,username,password);
    }

    public void query(String query, String accept, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {
        HttpRequest<Buffer> request = client
                .getAbs(uri + queryEndpoint)
                .addQueryParam("query", query);
        if (accept != null) {
            request.putHeader("Accept", accept);
        }
        query(request, handler);
    }



    public void query(HttpRequest<Buffer> request, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {
        if (breaker != null) {
            breaker.<HttpResponse<Buffer>>execute(promise -> send(request, HttpMethod.GET, promise))
                    .setHandler(ar -> {
                        if (ar.succeeded()) {
                            handler.handle(Future.succeededFuture(ar.result()));
                        } else {
                            handler.handle(Future.failedFuture(ar.cause()));
                        }
                    });
        } else {
            Promise<HttpResponse<Buffer>> promise = Promise.promise();
            send(request, HttpMethod.GET, promise);
            promise.future().setHandler(ar -> {
                if (ar.succeeded()) {
                    handler.handle(Future.succeededFuture(ar.result()));
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
        }
    }
}
