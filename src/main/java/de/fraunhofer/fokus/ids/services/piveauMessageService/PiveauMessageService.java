package de.fraunhofer.fokus.ids.services.piveauMessageService;

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
public interface PiveauMessageService {

    @Fluent
    PiveauMessageService createCatalogue(String body, String id, Handler<AsyncResult<Void>> readyHandler);

    @Fluent
    PiveauMessageService createDataSet(String body, String id, String catalogue, Handler<AsyncResult<Void>> readyHandler);

    @Fluent
    PiveauMessageService getAllDatasetsOfCatalogue(String catalogueId , Handler<AsyncResult<JsonObject>> readyHandler);

    @Fluent
    PiveauMessageService deleteDataSet(String id, String catalogue, Handler<AsyncResult<Void>> readyHandler);

    @Fluent
    PiveauMessageService deleteCatalogue(String id, Handler<AsyncResult<Void>> readyHandler);
    @GenIgnore
    static PiveauMessageService create(Vertx vertx, WebClient webClient, int gatewayPort, String gatewayHost, String apikey, Handler<AsyncResult<PiveauMessageService>> readyHandler) {
        return new PiveauMessageServiceImpl(vertx, webClient, gatewayPort, gatewayHost, apikey, readyHandler);
    }

    @GenIgnore
    static PiveauMessageService createProxy(Vertx vertx, String address) {
        return new PiveauMessageServiceVertxEBProxy(vertx, address);
    }

}
