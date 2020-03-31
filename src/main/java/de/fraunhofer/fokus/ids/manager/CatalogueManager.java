package de.fraunhofer.fokus.ids.manager;

import de.fraunhofer.fokus.ids.services.databaseService.DatabaseService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.List;

public class CatalogueManager {
    private DatabaseService databaseService;
    private final Logger LOGGER = LoggerFactory.getLogger(CatalogueManager.class.getName());

    private final static String FIND_CATALOGUES = "SELECT * FROM catalogues";
    private final static String INSERT_CAT_STATEMENT = "INSERT INTO catalogues (created_at, updated_at, external_id, internal_id) values (NOW(),NOW(),?,?)";
    private final static String DELETE_CAT_STATEMENT = "DELETE FROM catalogues WHERE internal_id = ?";
    private final static String SELECT_CAT_STATEMENT = "SELECT * FROM catalogues WHERE external_id=?";


    public CatalogueManager(Vertx vertx) {
        this.databaseService = DatabaseService.createProxy(vertx, "databaseService");
    }

    public void find(Handler<AsyncResult<List<JsonObject>>> resultHandler) {
        databaseService.query(FIND_CATALOGUES, new JsonArray(), catalogues -> {
            if (catalogues.succeeded()) {
                resultHandler.handle(Future.succeededFuture(catalogues.result()));
            } else {
                LOGGER.error(catalogues.cause());
                resultHandler.handle(Future.failedFuture(catalogues.cause()));
            }

        });
    }

    public void getCatalogueByExternalId(String externalId, Handler<AsyncResult<JsonObject>> resultHandler) {
        databaseService.query(SELECT_CAT_STATEMENT, new JsonArray().add(externalId), catalogues -> {
            if (catalogues.succeeded() && !catalogues.result().isEmpty()) {
                resultHandler.handle(Future.succeededFuture(catalogues.result().get(0)));
            } else {
                if(catalogues.cause() == null) {
                    LOGGER.error(catalogues.cause());
                }
                resultHandler.handle(Future.failedFuture(catalogues.cause()));
            }
        });
    }

    public void deleteByInternalId(String catalogueInternalId, Handler<AsyncResult<List<JsonObject>>> resultHandler) {
        databaseService.update(DELETE_CAT_STATEMENT, new JsonArray().add(catalogueInternalId), deleteCatalogueReply -> {
            if (deleteCatalogueReply.succeeded()) {
                LOGGER.info("Catalogue From Database successfully deleted");
                resultHandler.handle(Future.succeededFuture(deleteCatalogueReply.result()));
            } else {
                resultHandler.handle(Future.failedFuture(deleteCatalogueReply.cause()));
            }
        });
    }

    public void create(String internalId, String externalId, Handler<AsyncResult<Void>> resultHandler){
        databaseService.update(INSERT_CAT_STATEMENT, new JsonArray().add(internalId).add(externalId), reply -> {
            if (reply.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(reply.cause()));
                LOGGER.error(reply.cause());
            }
        });
    }
}