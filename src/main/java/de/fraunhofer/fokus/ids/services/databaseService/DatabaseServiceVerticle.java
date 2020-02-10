package de.fraunhofer.fokus.ids.services.databaseService;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.asyncsql.PostgreSQLClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.serviceproxy.ServiceBinder;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DatabaseServiceVerticle extends AbstractVerticle {

    private Logger LOGGER = LoggerFactory.getLogger(DatabaseServiceVerticle.class.getName());

    @Override
    public void start(Future<Void> startFuture) {

        ConfigStoreOptions confStore = new ConfigStoreOptions()
                .setType("env");

        ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(confStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        retriever.getConfig(ar -> {
            if (ar.succeeded()) {
                JsonObject env = ar.result();
                JsonObject config = new JsonObject()
                        .put("host", env.getString("HOST"))
                        .put("port", env.getLong("PORT"))
                        .put("username", env.getString("USER"))
                        .put("database", env.getString("DATABASE"))
                        .put("password", env.getString("PASSWORD"));

                SQLClient jdbc = PostgreSQLClient.createShared(vertx, config);
                DatabaseService.create(jdbc, ready -> {
                    if (ready.succeeded()) {
                        ServiceBinder binder = new ServiceBinder(vertx);
                        binder
                                .setAddress("databaseService")
                                .register(DatabaseService.class, ready.result());
                        LOGGER.info("Databaseservice successfully started.");
                        startFuture.complete();
                    } else {
                        LOGGER.error(ready.cause());
                        startFuture.fail(ready.cause());
                    }
                });
            } else {
                startFuture.fail(ar.cause());
                LOGGER.error("Config could not be retrieved.");
            }
        });
    }
}
