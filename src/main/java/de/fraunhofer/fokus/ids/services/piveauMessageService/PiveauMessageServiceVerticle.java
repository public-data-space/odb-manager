package de.fraunhofer.fokus.ids.services.piveauMessageService;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;

public class PiveauMessageServiceVerticle extends AbstractVerticle {
    private Logger LOGGER = LoggerFactory.getLogger(PiveauMessageServiceVerticle.class.getName());

    @Override
    public void start(Promise<Void> startPromise) {
        WebClient webClient = WebClient.create(vertx);

        ConfigStoreOptions confStore = new ConfigStoreOptions()
                .setType("env");

        ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(confStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        retriever.getConfig(ar -> {
            if (ar.succeeded()) {
                JsonObject config = ar.result().getJsonObject("PIVEAU_HUB_CONFIG");
                PiveauMessageService.create(vertx, webClient, config, ready -> {
                    if (ready.succeeded()) {
                        ServiceBinder binder = new ServiceBinder(vertx);
                        binder
                                .setAddress(PiveauMessageService.ADDRESS)
                                .register(PiveauMessageService.class, ready.result());
                        LOGGER.info("Datasourceadapterservice successfully started.");
                        startPromise.complete();
                    } else {
                        LOGGER.error(ready.cause());
                        startPromise.fail(ready.cause());
                    }
                });
            } else {
                LOGGER.error(ar.cause());
                startPromise.fail(ar.cause());
            }
        });
    }


}
