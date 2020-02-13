package de.fraunhofer.fokus.ids.utils;

import de.fraunhofer.fokus.ids.services.databaseService.DatabaseService;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;


public class InitService {

    private final Logger LOGGER = LoggerFactory.getLogger(InitService.class.getName());

    private DatabaseService databaseService;

    private final String CATALOGUE_TABLE_CREATE_QUERY = "CREATE TABLE IF NOT EXISTS catalogues (id SERIAL , created_at TIMESTAMP , updated_at TIMESTAMP , internal_id TEXT, external_id TEXT)";
    private final String DATASET_TABLE_CREATE_QUERY = "CREATE TABLE IF NOT EXISTS datasets (id SERIAL , created_at TIMESTAMP , updated_at TIMESTAMP , internal_id TEXT, external_id TEXT)";

    public InitService(Vertx vertx){
        this.databaseService = DatabaseService.createProxy(vertx, "databaseService");
    }

    public void initDatabase(Handler<AsyncResult<Void>> resultHandler){
        LOGGER.info("init");
        databaseService.update(CATALOGUE_TABLE_CREATE_QUERY, new JsonArray(), reply -> {});
        databaseService.update(DATASET_TABLE_CREATE_QUERY, new JsonArray(), reply -> {});
        resultHandler.handle(Future.succeededFuture());
    }
}
