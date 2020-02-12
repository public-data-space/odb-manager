package de.fraunhofer.fokus.ids.services.brokerMessageService;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.WebClient;

public class BrokerMessageServiceImpl implements BrokerMessageService{
    private Logger LOGGER = LoggerFactory.getLogger(BrokerMessageServiceImpl.class.getName());
    private WebClient webClient;
    private String piveauHost;
    private Vertx vertx;
    private int piveauPort;
    private String piveauAPIkey;

    public BrokerMessageServiceImpl(Vertx vertx, WebClient webClient, int piveauPort, String piveauHost, String piveauAPIkey, Handler<AsyncResult<BrokerMessageService>> readyHandler) {
        this.webClient = webClient;
        this.piveauHost = piveauHost;
        this.piveauPort = piveauPort;
        this.piveauAPIkey = piveauAPIkey;
        this.vertx = vertx;
        readyHandler.handle(Future.succeededFuture(this));
    }

    private void put(int port, String host, String path, String payload, Handler<AsyncResult<Void>> resultHandler) {
        webClient
                .put(port, host, path)
                .putHeader("content-type", "text/turtle")
                .putHeader("Authorization", piveauAPIkey)
                .sendBuffer(Buffer.buffer(payload), ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture());
                    } else {
                        LOGGER.error(ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    private void post(int port, String host, String path, String payload, Handler<AsyncResult<String>> resultHandler) {
        webClient
                .post(port, host, path)
                .putHeader("Content-Type","text/turtle")
                .putHeader("Authorization", piveauAPIkey)
                .sendBuffer(Buffer.buffer(payload), ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture());
                    } else {
                        LOGGER.error(ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    private void delete(int port, String host, String path, Handler<AsyncResult<String>> resultHandler) {
        webClient
                .delete(port, host, path)
                .putHeader("Content-Type","text/turtle")
                .putHeader("Authorization", piveauAPIkey)
                .send( ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar.result().bodyAsString()));
                    } else {
                        LOGGER.error(ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    @Override
    public BrokerMessageService createCatalogue(String body, String id, Handler<AsyncResult<BrokerMessageService>> readyHandler) {
        put(piveauPort,piveauHost,"/catalogues/"+id, body, jsonObjectAsyncResult -> {
            if (jsonObjectAsyncResult.succeeded()) {
                LOGGER.info("Catalogue "+id+ " successfully registered.");
                readyHandler.handle(Future.succeededFuture());
            }
            else {
                LOGGER.error(jsonObjectAsyncResult.cause());
                readyHandler.handle(Future.failedFuture(jsonObjectAsyncResult.cause()));
            }

        });
        return this;
    }

    @Override
    public BrokerMessageService createDataSet(String body, String id, String catalogue, Handler<AsyncResult<BrokerMessageService>> readyHandler) {
        put(piveauPort,piveauHost,"/datasets/"+id+"?catalogue="+catalogue, body, jsonObjectAsyncResult -> {
            if (jsonObjectAsyncResult.succeeded()) {
                LOGGER.info("Dataset "+id+ " successfully registered.");
                readyHandler.handle(Future.succeededFuture());
            }
            else {
                LOGGER.error(jsonObjectAsyncResult.cause());
                readyHandler.handle(Future.failedFuture(jsonObjectAsyncResult.cause()));
            }

        });
        return this;
    }

    @Override
    public BrokerMessageService deleteDataSet( String id, String catalogue, Handler<AsyncResult<BrokerMessageService>> readyHandler) {
        delete(piveauPort,piveauHost,"/datasets/"+id+"?catalogue="+catalogue, jsonObjectAsyncResult -> {
            if (jsonObjectAsyncResult.succeeded()) {
                LOGGER.info("Dataset "+id+ " successfully deleted.");
                readyHandler.handle(Future.succeededFuture());
            }
            else {
                LOGGER.error(jsonObjectAsyncResult.cause());
                readyHandler.handle(Future.failedFuture(jsonObjectAsyncResult.cause()));
            }
        });
        return this;
    }

    @Override
    public BrokerMessageService deleteCatalogue(String id, Handler<AsyncResult<BrokerMessageService>> readyHandler) {
        delete(piveauPort,piveauHost,"/catalogues/"+id, jsonObjectAsyncResult -> {
            if (jsonObjectAsyncResult.succeeded()) {
                LOGGER.info("Catalogue "+id+ " successfully deleted.");
                readyHandler.handle(Future.succeededFuture());
            }
            else {
                LOGGER.error(jsonObjectAsyncResult.cause());
                readyHandler.handle(Future.failedFuture(jsonObjectAsyncResult.cause()));
            }
        });
        return this;
    }
}
