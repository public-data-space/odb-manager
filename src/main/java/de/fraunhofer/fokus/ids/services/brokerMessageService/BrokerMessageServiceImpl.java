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

    public BrokerMessageServiceImpl(Vertx vertx,WebClient webClient,  int piveauPort,String piveauHost, Handler<AsyncResult<BrokerMessageService>> readyHandler) {
        this.webClient = webClient;
        this.piveauHost = piveauHost;
        this.piveauPort = piveauPort;
        this.vertx = vertx;
        readyHandler.handle(Future.succeededFuture(this));
    }

    private void post(int port, String host, String path, JsonObject payload, Handler<AsyncResult<String>> resultHandler) {
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

    @Override
    public BrokerMessageService sendBody(JsonObject body, String id) {
        post(piveauPort,piveauHost,"/catalogues/"+id,body,jsonObjectAsyncResult -> {
            if (jsonObjectAsyncResult.succeeded()) {
                System.out.println("Succeeded");

            }
            else {
                LOGGER.error(jsonObjectAsyncResult.cause());
                System.out.println("Error ");

            }

        });
        return this;
    }
}
