package de.fraunhofer.fokus.ids.services.piveauMessageService;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.WebClient;

public class PiveauMessageServiceImpl implements PiveauMessageService {
    private Logger LOGGER = LoggerFactory.getLogger(PiveauMessageServiceImpl.class.getName());
    private WebClient webClient;
    private String piveauHost;
    private Vertx vertx;
    private int piveauPort;
    private String piveauAPIkey;

    public PiveauMessageServiceImpl(Vertx vertx, WebClient webClient, JsonObject config, Handler<AsyncResult<PiveauMessageService>> readyHandler) {
        this.webClient = webClient;
        this.piveauHost = config.getString("host");
        this.piveauPort = config.getInteger("port");
        this.piveauAPIkey = config.getString("apiKey");
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

    private void get(int port, String host, String path, Handler<AsyncResult<JsonObject>> resultHandler) {

        webClient
                .get(port, host, path)
                .putHeader("Accept", "application/json")
                .send(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar.result().bodyAsJsonObject()));
                    } else {
                        LOGGER.error(ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    @Override
    public PiveauMessageService createCatalogue(String body, String id, Handler<AsyncResult<Void>> readyHandler) {
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
    public PiveauMessageService createDataSet(String body, String id, String catalogue, Handler<AsyncResult<Void>> readyHandler) {
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
    public PiveauMessageService getAllDatasetsOfCatalogue(String catalogueId , Handler<AsyncResult<JsonObject>> readyHandler) {
        get(piveauPort,piveauHost,"/datasets?catalogue="+catalogueId, jsonObjectAsyncResult -> {
            if(jsonObjectAsyncResult.succeeded()) {
                readyHandler.handle(Future.succeededFuture(jsonObjectAsyncResult.result()));   }
            else {
                LOGGER.error(jsonObjectAsyncResult.cause());
                readyHandler.handle(Future.failedFuture(jsonObjectAsyncResult.cause()));
            }
        });
        return this;
    }

    @Override
    public PiveauMessageService deleteDataSet(String id, String catalogue, Handler<AsyncResult<Void>> readyHandler) {
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
    public PiveauMessageService deleteCatalogue(String id, Handler<AsyncResult<Void>> readyHandler) {
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
