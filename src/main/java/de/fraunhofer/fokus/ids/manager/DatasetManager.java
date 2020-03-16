package de.fraunhofer.fokus.ids.manager;

import de.fraunhofer.fokus.ids.services.piveauMessageService.PiveauMessageService;
import de.fraunhofer.fokus.ids.services.databaseService.DatabaseService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DatasetManager {
    private DatabaseService databaseService;
    private PiveauMessageService piveauMessageService;
    private final Logger LOGGER = LoggerFactory.getLogger(DatasetManager.class.getName());

    private final static String SELECT_DS_STATEMENT = "SELECT * FROM datasets WHERE external_id=?";
    private final static String RESOLVE_DS_STATEMENT = "SELECT * FROM datasets WHERE internal_id=?";
    private static final String DELETE_DS_UPDATE = "DELETE FROM datasets WHERE internal_id = ?";
    private static final String DELETE_DS_EXTERNAL_UPDATE = "DELETE FROM datasets WHERE external_id = ?";
    private final static String INSERT_DS_STATEMENT = "INSERT INTO datasets (created_at, updated_at, external_id, internal_id) values (NOW(),NOW(),?,?)";

    public DatasetManager(Vertx vertx) {
        this.databaseService = DatabaseService.createProxy(vertx, "databaseService");
        this.piveauMessageService = PiveauMessageService.createProxy(vertx, "piveauMessageService");
    }

    public void deleteByInternalId(String internalId, Handler<AsyncResult> resultHandler) {
        databaseService.update(DELETE_DS_UPDATE, new JsonArray().add(internalId), reply -> {
            if (reply.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(reply.cause()));
                LOGGER.error(reply.cause());
            }
        });
    }

    public void deleteByExternalId(String externalId, Handler<AsyncResult> resultHandler) {
        databaseService.update(DELETE_DS_EXTERNAL_UPDATE, new JsonArray().add(externalId), reply -> {
            if (reply.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(reply.cause()));
                LOGGER.error(reply.cause());
            }
        });
    }

    public void findByInternalId(String internalId, Handler<AsyncResult<JsonObject>> resultHandler) {
        query(RESOLVE_DS_STATEMENT, new JsonArray().add(internalId), resultHandler);
    }

    public void findByExternalId(String externalId, Handler<AsyncResult<JsonObject>> resultHandler) {
        query(SELECT_DS_STATEMENT, new JsonArray().add(externalId), resultHandler);
    }

    private void query(String query, JsonArray array, Handler<AsyncResult<JsonObject>> resultHandler){
        databaseService.query(query, array, reply -> {
            if (reply.succeeded() && !reply.result().isEmpty()) {
                resultHandler.handle(Future.succeededFuture(reply.result().get(0)));
            } else {
                resultHandler.handle(Future.failedFuture(reply.cause()));
                LOGGER.error(reply.cause());
            }
        });
    }

    public void create(String internalId, String externalId,Handler<AsyncResult<Void>> resultHandler){
        databaseService.update(INSERT_DS_STATEMENT, new JsonArray().add(internalId).add(externalId), reply -> {
            if (reply.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(reply.cause()));
                LOGGER.error(reply.cause());
            }
        });
    }

    public void dataAssetIdsOfCatalogue(String catalogueInternalId, Handler<AsyncResult<List<String>>> asyncResultHandler) {
        piveauMessageService.getAllDatasetsOfCatalogue(catalogueInternalId, jsonReply -> {
            if (jsonReply.succeeded()) {
                ArrayList<String> ids = new ArrayList<>();
                if (!jsonReply.result().isEmpty()) {
                    for (Object jsonObject : jsonReply.result().getJsonArray("@graph")) {
                        JsonObject dataAsset = (JsonObject) jsonObject;
                        String idString = dataAsset.getString("@id");
                        String containsString = "https://ids.fokus.fraunhofer.de/set/data/";
                        if (idString.toLowerCase().contains(containsString.toLowerCase())) {
                            String dataAssetId = idString.substring(containsString.length());
                            ids.add(dataAssetId);
                        }
                    }
                }
                asyncResultHandler.handle(Future.succeededFuture(ids));
            } else {
                LOGGER.error("Can not get Ids of Catalogue");
                asyncResultHandler.handle(Future.failedFuture(jsonReply.cause()));
            }
        });
    }
}
