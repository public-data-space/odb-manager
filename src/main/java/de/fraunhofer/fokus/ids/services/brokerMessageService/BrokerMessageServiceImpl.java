package de.fraunhofer.fokus.ids.services.brokerMessageService;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
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

    public BrokerMessageServiceImpl(Vertx vertx, WebClient webClient, int piveauPort, String piveauHost, Handler<AsyncResult<BrokerMessageService>> readyHandler) {
        this.webClient = webClient;
        this.piveauHost = piveauHost;
        this.piveauPort = piveauPort;
        this.vertx = vertx;
        readyHandler.handle(Future.succeededFuture(this));
    }

    private void put(int port, String host, String path, JsonObject payload, Handler<AsyncResult<String>> resultHandler) {
        webClient
                .put(port, host, path)
                .sendJsonObject(payload, ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar.result().bodyAsString()));
                    } else {
                        LOGGER.error(ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    private void post(int port, String host, String path, JsonObject payload, Handler<AsyncResult<String>> resultHandler) {
        webClient
                .post(port, host, path)
                .sendJsonObject(payload, ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar.result().bodyAsString()));
                    } else {
                        LOGGER.error(ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    private void delete(int port, String host, String path, Handler<AsyncResult<String>> resultHandler) {
        webClient
                .delete(port, host, path).send( ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar.result().bodyAsString()));
                    } else {
                        LOGGER.error(ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    @Override
    public BrokerMessageService createCatalogue(JsonObject body, String id, Handler<AsyncResult<BrokerMessageService>> readyHandler) {
        put(piveauPort,piveauHost,"/catalogues/"+id, body, jsonObjectAsyncResult -> {
            if (jsonObjectAsyncResult.succeeded()) {
                LOGGER.info("Succeeded");
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
    public BrokerMessageService createDataSet(JsonObject body, String id, String catalogue, Handler<AsyncResult<BrokerMessageService>> readyHandler) {
        put(piveauPort,piveauHost,"/datasets/"+id+"?catalogue="+catalogue, body, jsonObjectAsyncResult -> {
            if (jsonObjectAsyncResult.succeeded()) {
                LOGGER.info("Succeeded");
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
    public BrokerMessageService createDistribution(JsonObject body, String dataset, String catalogue, Handler<AsyncResult<BrokerMessageService>> readyHandler) {
        post(piveauPort,piveauHost,"/distributions?dataset="+dataset+"&catalogue="+catalogue, body, jsonObjectAsyncResult -> {
            if (jsonObjectAsyncResult.succeeded()) {
                LOGGER.info("Succeeded");
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
                LOGGER.info("Succeeded");
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
                LOGGER.info("Succeeded");
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
