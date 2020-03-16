package de.fraunhofer.fokus.ids.controller;

import de.fraunhofer.fokus.ids.manager.CatalogueManager;
import de.fraunhofer.fokus.ids.manager.DatasetManager;
import de.fraunhofer.fokus.ids.manager.GraphManager;
import de.fraunhofer.fokus.ids.services.IDSService;
import de.fraunhofer.fokus.ids.services.piveauMessageService.PiveauMessageService;
import de.fraunhofer.fokus.ids.services.dcatTransformerService.DCATTransformerService;
import de.fraunhofer.iais.eis.Connector;
import de.fraunhofer.iais.eis.RejectionReason;
import de.fraunhofer.iais.eis.Resource;
import de.fraunhofer.iais.eis.StaticEndpoint;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class UpdateController {
    private Logger LOGGER = LoggerFactory.getLogger(UpdateController.class.getName());

    private GraphManager graphManager;
    private CatalogueManager catalogueManager;
    private DatasetManager datasetManager;
    private IDSService idsService;
    private PiveauMessageService piveauMessageService;
    private DCATTransformerService dcatTransformerService;

    public UpdateController(Vertx vertx, GraphManager graphManager){
        this.graphManager = graphManager;
        this.catalogueManager = new CatalogueManager(vertx);
        this.datasetManager = new DatasetManager(vertx);
        this.idsService = new IDSService(vertx);
        this.piveauMessageService = PiveauMessageService.createProxy(vertx, "piveauMessageService");
        this.dcatTransformerService = DCATTransformerService.createProxy(vertx, "dcatTransformerService");
    }

    public void update(URI uri, Connector connector, Handler<AsyncResult<String>> readyHandler) {

        catalogueManager.getCatalogueByExternalId(connector.getId().toString(), catalogueIdResult -> {
            if (catalogueIdResult.succeeded()) {
                graphManager.update(connector.getId().toString(),Json.encode(connector),next1->
                        dcatTransformerService.transformCatalogue(Json.encode(connector), null, catalogueTransformationResult -> {
                            createCatalogueInPiveau(catalogueTransformationResult, catalogueIdResult.result().getString("internal_id"), next2 -> {
                                updateDatasets(connector,uri, catalogueIdResult.result().getString("internal_id"), readyHandler);
                            });
                    }));
            } else {
                LOGGER.error(catalogueIdResult.cause());
                idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
            }
        });
    }

        private void createCatalogueInPiveau(AsyncResult<String> catalogue, String catalogueId, Handler<AsyncResult<String>> next) {
        if (catalogue.succeeded()) {
            piveauMessageService.createCatalogue(catalogue.result(), catalogueId, catalogueReply -> {
                if (catalogueReply.succeeded()) {
                    next.handle(Future.succeededFuture(catalogueId));
                } else {
                    LOGGER.error(catalogueReply.cause());
                    next.handle(Future.failedFuture(catalogueReply.cause()));
                }
            });
        } else {
            next.handle(Future.failedFuture(catalogue.cause()));
        }
    }

    private void updateDatasets(Connector connector, URI uri, String catalogueId, Handler<AsyncResult<String>> readyHandler) {
        datasetManager.dataAssetIdsOfCatalogue(catalogueId, piveauDatasetIds ->
                resolvePiveauIds(piveauDatasetIds, result -> {
            if (result.succeeded()) {
                java.util.Map<String, Future> datasetUpdateFutures = new HashMap<>();
                Set<String> availableDatasetIds = new HashSet(result.result().keySet());

                for (String id :availableDatasetIds) {
                    graphManager.delete(id, r -> {
                        if(r.failed()){
                            LOGGER.info("Deletion of dataset graph failed.");
                        }
                    });
                }
                Set<String> messageDatasetIds = connector.getCatalog().getOffer().stream().map(r -> r.getId().toString()).collect(Collectors.toSet());
                Map<String, Resource> id2ResourceMap = new HashMap<>();

                for(String id : messageDatasetIds){
                    Resource dataset = connector.getCatalog().getOffer().stream().filter(r -> ((Resource) r).getId().toString().equals(id)).collect(Collectors.toList()).get(0);
                    id2ResourceMap.put(id, dataset);

                    graphManager.create(uri.toString(),Json.encode(dataset),r->{
                        if(r.failed()){
                            LOGGER.info("Deletion of dataset graph failed.");
                        }
                    });
                    availableDatasetIds.add(id);
                }
                for (String datasetId : availableDatasetIds) {
                    datasetUpdateFutures.put(datasetId, Future.future());
                }

                Collection<String> piveauIds = result.result().keySet();
                if (piveauIds.isEmpty() && messageDatasetIds.isEmpty()) {
                    idsService.handleSucceededMessage(uri, readyHandler);
                } else {
                    for (String messageId : messageDatasetIds) {
                        Resource dataset = id2ResourceMap.get(messageId);
                        dcatTransformerService.transformDataset(Json.encode(dataset), ((StaticEndpoint)dataset.getResourceEndpoint().get(0)).getEndpointArtifact().getCreationDate().toString(), datasetTransformResult -> {
                            if(datasetTransformResult.succeeded()) {
                                if (piveauIds.contains(messageId)) {
                                    updateDataset(datasetTransformResult.result(), messageId, catalogueId, datasetUpdateFutures);
                                } else {
                                    String internalId = UUID.randomUUID().toString();
                                    createDataSet(datasetTransformResult.result(), messageId, internalId, catalogueId, datasetUpdateFutures);
                                }
                                piveauIds.remove(messageId);
                            } else {
                                LOGGER.error(datasetTransformResult.cause());
                                idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                            }
                        });
                    }
                    CompositeFuture.all(messageDatasetIds.stream().map(id -> datasetUpdateFutures.get(id)).collect(Collectors.toList())).setHandler( mesFutures -> {
                        if(mesFutures.succeeded()) {
                            for (String orphan : piveauIds) {
                                deleteDatasetPiveau(result.result().get(orphan), catalogueId, res -> datasetManager.deleteByExternalId(orphan, deleteResult -> handleDataSetFuture(deleteResult, orphan, datasetUpdateFutures)));
                            }
                        } else {
                            LOGGER.error(mesFutures.cause());
                            idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                        }
                    });
                    CompositeFuture.all(new ArrayList<>(datasetUpdateFutures.values())).setHandler(ac -> {
                        if (ac.succeeded()) {
                            idsService.handleSucceededMessage(uri, readyHandler);
                        } else {
                            idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                        }
                    });
                }
            } else {
                idsService.handleRejectionMessage(RejectionReason.NOT_FOUND, uri, readyHandler);
            }
        }));

    }

    public void createDataSet(String transformedDataset, String datasetExternalId, String dataSetId, String catalogueId, java.util.Map<String, Future> datasetUpdateFutures) {
        piveauMessageService.createDataSet(transformedDataset, dataSetId, catalogueId, datasetReply -> {
            if (datasetReply.succeeded()) {
                datasetManager.create(datasetExternalId, dataSetId, datasetPersistenceReply2 -> {
                    if (datasetPersistenceReply2.succeeded()) {
                        datasetUpdateFutures.get(datasetExternalId).complete();
                    } else {
                        datasetUpdateFutures.get(datasetExternalId).fail(datasetPersistenceReply2.cause());
                    }
                });
            } else {
                LOGGER.error(datasetReply.cause());
            }
        });
    }

    private void updateDataset(String datasetTTL, String messageId, String catalogueId, java.util.Map<String, Future> datasetUpdateFutures) {
        resolveDatasetIdForUpdate(messageId, reply -> {
            piveauMessageService.createDataSet(datasetTTL, reply.result(), catalogueId, datasetReply -> {
                if (datasetReply.succeeded()) {
                    datasetUpdateFutures.get(messageId).complete();
                } else {
                    LOGGER.error(datasetReply.cause());
                    datasetUpdateFutures.get(messageId).fail(datasetReply.cause());
                }
            });
        });
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

    private void handleDataSetFuture(AsyncResult<Void> reply, String idsId, java.util.Map<String, Future> datasetdeleteFutures) {
        if (reply.succeeded()) {
            datasetdeleteFutures.get(idsId).complete();
            LOGGER.info("DataAsset From Database successfully deleted");
        } else {
            LOGGER.error(reply.cause());
            datasetdeleteFutures.get(idsId).fail(reply.cause());
        }
    }

    private void resolvePiveauIds(AsyncResult<java.util.List<String>> piveauDatasetIds, Handler<AsyncResult<java.util.Map<String, String>>> completer) {
        java.util.Map<String, Future<JsonObject>> piveau2IDSResolveFutureMap = new HashMap<>();
        if (piveauDatasetIds.succeeded()) {
            for (String piveauId : piveauDatasetIds.result()) {
                Future<JsonObject> idsResolve = Future.future();
                piveau2IDSResolveFutureMap.put(piveauId, idsResolve);
                datasetManager.findByInternalId(piveauId, idsResolve.completer());
            }
            CompositeFuture.all(new ArrayList<>(piveau2IDSResolveFutureMap.values())).setHandler(ac -> {
                if (ac.succeeded()) {
                    java.util.Map<String, String> resultMap = new HashMap<>();
                    for (String piveauId : piveau2IDSResolveFutureMap.keySet()) {
                        resultMap.put(piveau2IDSResolveFutureMap.get(piveauId).result().getString("external_id"), piveauId);
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

    private void resolveDatasetIdForUpdate(String dataassetIdExternal, Handler<AsyncResult<String>> next) {
        datasetManager.findByExternalId(dataassetIdExternal, datasetPersistenceReply -> {
            if (datasetPersistenceReply.succeeded()) {
                if (!datasetPersistenceReply.result().isEmpty()) {
                    String id = datasetPersistenceReply.result().getString("internal_id");
                    next.handle(Future.succeededFuture(id));
                } else {
                    String datId = UUID.randomUUID().toString();
                    next.handle(Future.succeededFuture(datId));
                }
            } else {
                LOGGER.error(datasetPersistenceReply.cause());
                next.handle(Future.failedFuture(datasetPersistenceReply.cause()));
            }
        });
    }
}
