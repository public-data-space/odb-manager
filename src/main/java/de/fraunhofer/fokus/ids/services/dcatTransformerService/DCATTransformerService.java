package de.fraunhofer.fokus.ids.services.dcatTransformerService;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

@ProxyGen
@VertxGen
public interface DCATTransformerService {

    @Fluent
    DCATTransformerService transformCatalogue(String connectorJson, Handler<AsyncResult<String>> readyHandler);

    @Fluent
    DCATTransformerService transformDataset(String datasetJson, Handler<AsyncResult<String>> readyHandler);

    @GenIgnore
    static DCATTransformerService create(Handler<AsyncResult<DCATTransformerService>> readyHandler) {
        return new DCATTransformerServiceImpl(readyHandler);
    }

    @GenIgnore
    static DCATTransformerService createProxy(Vertx vertx, String address) {
        return new DCATTransformerServiceVertxEBProxy(vertx, address);
    }
}
