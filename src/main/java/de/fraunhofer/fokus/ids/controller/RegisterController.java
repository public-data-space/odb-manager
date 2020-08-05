package de.fraunhofer.fokus.ids.controller;

import de.fraunhofer.fokus.ids.manager.CatalogueManager;
import de.fraunhofer.fokus.ids.manager.DatasetManager;
import de.fraunhofer.fokus.ids.manager.GraphManager;
import de.fraunhofer.fokus.ids.services.IDSService;
import de.fraunhofer.fokus.ids.services.piveauMessageService.PiveauMessageService;
import de.fraunhofer.fokus.ids.services.dcatTransformerService.DCATTransformerService;
import de.fraunhofer.fokus.ids.utils.TSConnector;
import de.fraunhofer.iais.eis.*;
import de.fraunhofer.iais.eis.ids.jsonld.Serializer;
import io.vertx.core.*;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.UUID;
import java.util.stream.Collectors;

public class RegisterController {
    private Logger LOGGER = LoggerFactory.getLogger(RegisterController.class.getName());

    private GraphManager graphManager;
    private CatalogueManager catalogueManager;
    private DatasetManager datasetManager;
    private IDSService idsService;
    private PiveauMessageService piveauMessageService;
    private DCATTransformerService dcatTransformerService;
    private Serializer serializer = new Serializer();

    public RegisterController(Vertx vertx, GraphManager graphManager,TSConnector tsConnector){
        this.graphManager = graphManager;
        this.catalogueManager = new CatalogueManager(vertx);
        this.datasetManager = new DatasetManager(vertx);
        this.idsService = new IDSService(vertx,tsConnector);
        this.piveauMessageService = PiveauMessageService.createProxy(vertx, PiveauMessageService.ADDRESS);
        this.dcatTransformerService = DCATTransformerService.createProxy(vertx, DCATTransformerService.ADDRESS);
    }

    public void registerResourceAvailableMessage(URI uri, String issuerConnector, Resource resource, Handler<AsyncResult<String>> readyHandle) {
        catalogueManager.getCatalogueByExternalId(issuerConnector, next -> {
            if (next.succeeded()) {
                String cataloguePiveauId = next.result().getString("internal_id");
                LOGGER.info("Katalog with id " + cataloguePiveauId + " found ");
                if (resource != null) {
                    datasetManager.findByExternalId(resource.getId().toString(), datasetIdreply -> {
                        if (datasetIdreply.succeeded()) {
                            LOGGER.info("Dataset " + resource.getId().toString() + " is already registered in the internal database. Rejecting ResrouceAvailableMessage.");
                            idsService.handleRejectionMessage(RejectionReason.BAD_PARAMETERS, uri, readyHandle);
                        } else {
                            java.util.Map<String, Promise> dataassetCreatePromises = new HashMap<>();
                            saveDatasetInDatabase(uri,cataloguePiveauId,resource,dataassetCreatePromises,readyHandle);
                            composeAllPromises(uri,readyHandle,dataassetCreatePromises);
                        }
                    });
                }
            } else {
                LOGGER.info("Katalog with id " + issuerConnector + " not found ");
                idsService.handleRejectionMessage(RejectionReason.BAD_PARAMETERS, uri, readyHandle);
            }
        });
    }
    public void register(URI uri, Connector connector, Handler<AsyncResult<String>> readyHandler) {
        String catalogueId = UUID.randomUUID().toString();
        catalogueManager.getCatalogueByExternalId(connector.getId().toString(),next->{
            if (next.succeeded()) {
                LOGGER.info("Connector is already registered in the internal database. Rejecting AvailableMessage.");
                idsService.handleRejectionMessage(RejectionReason.BAD_PARAMETERS, uri, readyHandler);
            }
            else {
                try {
                    graphManager.create(connector.getId().toString(), serializer.serialize(connector), graphCreationResult -> {
                        if (graphCreationResult.succeeded()) {
                            try {
                                dcatTransformerService.transformCatalogue(serializer.serialize(connector), null, catalogueTTLResult -> {
                                    if (catalogueTTLResult.succeeded()) {
                                        piveauMessageService.createCatalogue(catalogueTTLResult.result(), catalogueId, piveauCatalogueReply ->
                                                createCatalogueInternal(piveauCatalogueReply, catalogueId, connector.getId().toString(), internalCatalogueReply ->
                                                        handleDatasetCreation(internalCatalogueReply, uri, connector, catalogueId, readyHandler)));
                                    }
                                });
                            } catch (IOException e) {
                                LOGGER.error(e);
                                readyHandler.handle(Future.failedFuture(e));
                            }
                        } else {
                            LOGGER.error(graphCreationResult.cause());
                            idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                        }
                    });
                } catch (Exception e){
                    LOGGER.error(e);
                    readyHandler.handle(Future.failedFuture(e));
                }
            }
        });

    }

    private void createCatalogueInternal(AsyncResult<Void> reply, String catalogueId, String connectorId, Handler<AsyncResult<Void>> next) {
        if (reply.succeeded()) {
            catalogueManager.create(connectorId, catalogueId, cataloguePersistenceReply -> {
                if (cataloguePersistenceReply.succeeded()) {
                    next.handle(Future.succeededFuture());
                } else {
                    LOGGER.error(cataloguePersistenceReply.cause());
                    next.handle(Future.failedFuture(cataloguePersistenceReply.cause()));
                }
            });
        } else {
            next.handle(Future.failedFuture(reply.cause()));
        }
    }
    private void saveDatasetInDatabase(URI uri, String catalogueId, Resource resource, java.util.Map<String, Promise> dataassetCreatePromises, Handler<AsyncResult<String>> readyHandler) {
        StaticEndpoint staticEndpoint = (StaticEndpoint) resource.getResourceEndpoint().get(0);
        String date = staticEndpoint.getEndpointArtifact().getCreationDate().toString();
        try {
            graphManager.create(resource.getId().toString(), serializer.serialize(resource), graphResult -> {
                if (graphResult.succeeded()) {
                    try {
                        dcatTransformerService.transformDataset(serializer.serialize(resource), date, dataSetTransformResult -> {
                            if (dataSetTransformResult.succeeded()) {
                                dataassetCreatePromises.put(resource.getId().toString(), Promise.promise());
                                String datasetId = UUID.randomUUID().toString();
                                createDataSet(dataSetTransformResult.result(), resource.getId().toString(), datasetId, catalogueId, dataassetCreatePromises);
                            } else {
                                LOGGER.error(dataSetTransformResult.cause());
                                idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                            }
                        });
                    } catch (IOException e) {
                        LOGGER.error(e);
                        readyHandler.handle(Future.failedFuture(e));
                    }
                } else {
                    LOGGER.error(graphResult.cause());
                    idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                }
            });
        } catch (IOException e) {
            LOGGER.error(e);
            readyHandler.handle(Future.failedFuture(e));
        }
    }

    private void composeAllPromises(URI uri, Handler<AsyncResult<String>> readyHandler, java.util.Map<String, Promise> dataassetCreatePromises) {
        CompositeFuture.all(dataassetCreatePromises.values().stream().map(Promise::future).collect(Collectors.toList())).setHandler(ac -> {
            if (ac.succeeded()) {
                idsService.handleSucceededMessage(uri, readyHandler);
            } else {
                LOGGER.error(ac.cause());
                idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
            }
        });
    }

    private void handleDatasetCreation( AsyncResult<Void> internalCatalogueCreationReply, URI uri, Connector connector, String catalogueId, Handler<AsyncResult<String>> readyHandler) {
        if (internalCatalogueCreationReply.succeeded()) {
                    java.util.Map<String, Promise> dataassetCreatePromises = new HashMap<>();
                    if (connector.getCatalog() != null) {
                        for (Resource resource : connector.getCatalog().getOffer()) {
                            saveDatasetInDatabase(uri,catalogueId,resource,dataassetCreatePromises,readyHandler);
                        }
                        composeAllPromises(uri,readyHandler,dataassetCreatePromises);
                    }
        } else {
            idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
        }
    }

    private void createDataSet(String transformedDataset, String datasetExternalId, String dataSetId, String catalogueId, java.util.Map<String, Promise> datasetUpdateFutures) {
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
}
