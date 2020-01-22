package de.fraunhofer.fokus.ids.services.brokerMessageService;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;

public class BrokerMessageServiceVerticle extends AbstractVerticle {
    private Logger LOGGER = LoggerFactory.getLogger(BrokerMessageServiceVerticle.class.getName());

    @Override
    public void start(Future<Void> startFuture) {
        WebClient webClient = WebClient.create(vertx);

        ConfigStoreOptions confStore = new ConfigStoreOptions()
                .setType("env");

        ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(confStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        retriever.getConfig(ar -> {
            if (ar.succeeded()) {
                BrokerMessageService.create(vertx, webClient, ar.result().getInteger("PIVEAU_HUB_PORT"), ar.result().getString("PIVEAU_HUB_HOST"),  ready -> {
                    if (ready.succeeded()) {
                        ServiceBinder binder = new ServiceBinder(vertx);
                        binder
                                .setAddress("brokerMessageService")
                                .register(BrokerMessageService.class, ready.result());
                        LOGGER.info("Datasourceadapterservice successfully started.");
                        startFuture.complete();
                    } else {
                        LOGGER.error(ready.cause());
                        startFuture.fail(ready.cause());
                    }
                });
            } else {
                LOGGER.error(ar.cause());
                startFuture.fail(ar.cause());
            }
        });
    }


}
