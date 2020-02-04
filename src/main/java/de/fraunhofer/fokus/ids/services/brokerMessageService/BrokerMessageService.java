package de.fraunhofer.fokus.ids.services.brokerMessageService;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;

@ProxyGen
@VertxGen
public interface BrokerMessageService {

    @Fluent
    BrokerMessageService createCatalogue(JsonObject body, String id, Handler<AsyncResult<BrokerMessageService>> readyHandler);

    @Fluent
    BrokerMessageService createDataSet(JsonObject body, String id, String catalogue, Handler<AsyncResult<BrokerMessageService>> readyHandler);

    @GenIgnore
    static BrokerMessageService create(Vertx vertx, WebClient webClient, int gatewayPort, String gatewayHost, Handler<AsyncResult<BrokerMessageService>> readyHandler) {
        return new BrokerMessageServiceImpl(vertx, webClient, gatewayPort, gatewayHost, readyHandler);
    }

    @GenIgnore
    static BrokerMessageService createProxy(Vertx vertx, String address) {
        return new BrokerMessageServiceVertxEBProxy(vertx, address);
    }

}
