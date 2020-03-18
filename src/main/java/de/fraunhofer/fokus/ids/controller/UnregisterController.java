package de.fraunhofer.fokus.ids.controller;

import de.fraunhofer.fokus.ids.manager.CatalogueManager;
import de.fraunhofer.fokus.ids.manager.DatasetManager;
import de.fraunhofer.fokus.ids.manager.GraphManager;
import de.fraunhofer.fokus.ids.services.IDSService;
import de.fraunhofer.fokus.ids.services.piveauMessageService.PiveauMessageService;
import de.fraunhofer.fokus.ids.utils.TSConnector;
import de.fraunhofer.iais.eis.Connector;
import de.fraunhofer.iais.eis.RejectionReason;
import de.fraunhofer.iais.eis.Resource;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

public class UnregisterController {

    private Logger LOGGER = LoggerFactory.getLogger(UnregisterController.class.getName());

    private GraphManager graphManager;
    private CatalogueManager catalogueManager;
    private DatasetManager datasetManager;
    private IDSService idsService;
    private PiveauMessageService piveauMessageService;
    private TSConnector tsConnector ;

    public UnregisterController(Vertx vertx, GraphManager graphManager,TSConnector tsConnector){
        this.graphManager = graphManager;
        this.catalogueManager = new CatalogueManager(vertx);
        this.datasetManager = new DatasetManager(vertx);
        this.idsService = new IDSService(vertx,tsConnector);
        this.piveauMessageService = PiveauMessageService.createProxy(vertx, "piveauMessageService");
    }


    public void unregister(URI uri, Connector connector, Handler<AsyncResult<String>> readyHandler) {
        java.util.Map<String, Promise> datasetDeletePromises = new HashMap<>();

        catalogueManager.getCatalogueByExternalId(connector.getId().toString(), catalogueIdResult -> {
            if (catalogueIdResult.succeeded()) {
                String cataloguePiveauId = catalogueIdResult.result().getString("internal_id");
                graphManager.delete(connector.getId().toString(),reply->{});
                datasetManager.dataAssetIdsOfCatalogue(cataloguePiveauId, piveauDatasetIds -> {

                    resolvePiveauIds(piveauDatasetIds,mapAsyncResult -> {
                        for (String externalId : mapAsyncResult.result().keySet()){
                            graphManager.delete(externalId,reply->{});
                        }});

                    if (piveauDatasetIds.succeeded()) {
                        if (!piveauDatasetIds.result().isEmpty()) {
                            for (String id : piveauDatasetIds.result()) {
                                Promise datasetDeletePromise = Promise.promise();
                                datasetDeletePromises.put(id, datasetDeletePromise);
                                deleteDatasetPiveau(id, cataloguePiveauId, next -> datasetManager.deleteByInternalId(id, result -> handleDataSetFuture(result, id, datasetDeletePromises)));
                            }
                        }
                        for (Resource dataasset : connector.getCatalog().getOffer()) {
                            Promise datasetDeleteFuture = Promise.promise();
                            datasetDeletePromises.put(dataasset.getId().toString(), datasetDeleteFuture);
                            datasetManager.findByExternalId(dataasset.getId().toString(), datasetIdreply -> {
                                if (datasetIdreply.succeeded()) {
                                    if (!datasetIdreply.result().isEmpty()) {
                                        String datasePiveautId = datasetIdreply.result().getString("internal_id");
                                        String datasetIdsId = datasetIdreply.result().getString("external_id");
                                        deleteDatasetPiveau(datasePiveautId, cataloguePiveauId, externalDeleteReply ->
                                                deleteDatasetInternal(externalDeleteReply, datasePiveautId, datasetIdsId, datasetDeletePromises));
                                    } else {
                                        datasetDeleteFuture.complete();
                                    }
                                } else {
                                    LOGGER.error(datasetIdreply.cause());
                                    idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                                }
                            });
                        }
                        handleCatalogue(uri, new ArrayList<>(datasetDeletePromises.values()), cataloguePiveauId, readyHandler);
                    } else {
                        LOGGER.error(piveauDatasetIds.cause());
                        idsService.handleRejectionMessage(RejectionReason.NOT_FOUND, uri, readyHandler);
                    }
                });
            } else {
                idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
            }
        });
    }

    private void handleCatalogue(URI uri, java.util.List<Promise> datasetDeletePromises, String catalogueIdResult, Handler<AsyncResult<String>> readyHandler) {
        CompositeFuture.all(datasetDeletePromises.stream().map(Promise::future).collect(Collectors.toList())).setHandler(reply -> {
            if (reply.succeeded()) {
                deleteCatalogueExternal(reply, catalogueIdResult, externalCatalogueDeleteReply ->
                        deleteCatalogueInternal(uri, externalCatalogueDeleteReply, catalogueIdResult, readyHandler));
            } else {
                idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
            }
        });
    }

    private void deleteDatasetInternal(AsyncResult<Void> reply, String datasetPiveauId, String datasetIDSId, java.util.Map<String, Promise> datasetDeletePromises) {
        if (reply.succeeded()) {
            datasetManager.deleteByInternalId(datasetPiveauId, internalDatasetDeleteResult -> {
                if (reply.succeeded()) {
                    datasetDeletePromises.get(datasetIDSId).complete();
                } else {
                    datasetDeletePromises.get(datasetIDSId).fail(reply.cause());
                    LOGGER.error(reply.cause());
                }
            });
        } else {
            datasetDeletePromises.get(datasetPiveauId).fail(reply.cause());
        }
    }

    private void deleteCatalogueExternal(AsyncResult<CompositeFuture> reply, String catalogueInternalId, Handler<AsyncResult> next) {
        if (reply.succeeded()) {
            piveauMessageService.deleteCatalogue(catalogueInternalId, deleteCatalogueReply -> {
                if (deleteCatalogueReply.succeeded()) {
                    next.handle(Future.succeededFuture());
                } else {
                    LOGGER.error(deleteCatalogueReply.cause());
                    next.handle(Future.failedFuture(deleteCatalogueReply.cause()));
                }
            });
        } else {
            next.handle(Future.failedFuture(reply.cause()));
        }
    }

    private void deleteCatalogueInternal(URI uri, AsyncResult<Void> reply, String catalogueInternalId, Handler<AsyncResult<String>> readyHandler) {
        if (reply.succeeded()) {
            catalogueManager.deleteByInternalId(catalogueInternalId, deleteCatalogueReply -> {
                if (deleteCatalogueReply.succeeded()) {
                    idsService.handleSucceededMessage(uri, readyHandler);
                } else {
                    LOGGER.error(deleteCatalogueReply.cause());
                    idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                }
            });
        } else {
            LOGGER.error(reply.cause());
            idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
        }
    }

    private void deleteDatasetPiveau(String datasetId, String catalogueId, Handler<AsyncResult<Void>> next) {
        piveauMessageService.deleteDataSet(datasetId, catalogueId, deleteAsset -> {
            if (deleteAsset.succeeded()) {
                next.handle(Future.succeededFuture());
            } else {
                LOGGER.error(deleteAsset.cause());
                next.handle(Future.failedFuture(deleteAsset.cause()));
            }
        });
    }

    private void handleDataSetFuture(AsyncResult<Void> reply, String idsId, java.util.Map<String, Promise> datasetdeletePromises) {
        if (reply.succeeded()) {
            datasetdeletePromises.get(idsId).complete();
            LOGGER.info("DataAsset From Database successfully deleted");
        } else {
            LOGGER.error(reply.cause());
            datasetdeletePromises.get(idsId).fail(reply.cause());
        }
    }

    private void resolvePiveauIds(AsyncResult<java.util.List<String>> piveauDatasetIds, Handler<AsyncResult<java.util.Map<String, String>>> completer) {
        java.util.Map<String, Promise<JsonObject>> piveau2IDSResolvePromiseMap = new HashMap<>();
        if (piveauDatasetIds.succeeded()) {
            for (String piveauId : piveauDatasetIds.result()) {
                Promise<JsonObject> idsResolve = Promise.promise();
                piveau2IDSResolvePromiseMap.put(piveauId, idsResolve);
                datasetManager.findByInternalId(piveauId, idsResolve);
            }
            CompositeFuture.all(piveau2IDSResolvePromiseMap.values().stream().map(Promise::future).collect(Collectors.toList())).setHandler(ac -> {
                if (ac.succeeded()) {
                    java.util.Map<String, String> resultMap = new HashMap<>();
                    for (String piveauId : piveau2IDSResolvePromiseMap.keySet()) {
                        resultMap.put(piveau2IDSResolvePromiseMap.get(piveauId).future().result().getString("external_id"), piveauId);
                    }
                    completer.handle(Future.succeededFuture(resultMap));
                } else {
                    completer.handle(Future.failedFuture(ac.cause()));
                }
            });
        } else {
            completer.handle(Future.failedFuture(piveauDatasetIds.cause()));
        }
    }
}
