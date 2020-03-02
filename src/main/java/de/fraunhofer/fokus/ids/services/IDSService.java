package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.services.brokerMessageService.BrokerMessageService;
import de.fraunhofer.fokus.ids.services.databaseService.DatabaseService;
import de.fraunhofer.fokus.ids.services.dcatTransformerService.DCATTransformerService;
import de.fraunhofer.fokus.ids.utils.IDSMessageParser;
import de.fraunhofer.iais.eis.*;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.StringBody;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class IDSService {
    private final Logger LOGGER = LoggerFactory.getLogger(IDSService.class.getName());
    private DatabaseService databaseService;
    private BrokerMessageService brokerMessageService;
    private DCATTransformerService dcatTransformerService;


    private final static String INSERT_CAT_STATEMENT = "INSERT INTO catalogues (created_at, updated_at, external_id, internal_id) values (NOW(),NOW(),?,?)";
    private final static String INSERT_DS_STATEMENT = "INSERT INTO datasets (created_at, updated_at, external_id, internal_id) values (NOW(),NOW(),?,?)";
    private final static String SELECT_CAT_STATEMENT = "SELECT * FROM catalogues WHERE external_id=?";
    private final static String SELECT_DS_STATEMENT = "SELECT * FROM datasets WHERE external_id=?";
    private final static String RESOLVE_DS_STATEMENT = "SELECT * FROM datasets WHERE internal_id=?";
    private static final String DELETE_DS_UPDATE = "DELETE FROM datasets WHERE internal_id = ?";
    private final static String DELETE_CAT_STATEMENT = "DELETE FROM catalogues WHERE internal_id = ?";

    private String INFO_MODEL_VERSION = "2.0.0";
    private String[] SUPPORTED_INFO_MODEL_VERSIONS = {"2.0.0"};

    public IDSService(Vertx vertx) {
        this.databaseService = DatabaseService.createProxy(vertx, "databaseService");
        this.brokerMessageService = BrokerMessageService.createProxy(vertx, "brokerMessageService");
        this.dcatTransformerService = DCATTransformerService.createProxy(vertx, "dcatTransformerService");
    }

    public void selfDescriptionRequest(URI uri, Handler<AsyncResult<String>> resultHandler) {
        JsonObject jsonObject = new JsonObject();
        SelfDescriptionResponse selfDescriptionResponse = buildSelfDescriptionResponse(jsonObject);
        Future<Broker> brokerFuture = Future.future();
        buildBroker(jsonObject, brokerFuture.completer());
        handleBrokerSelfDescription(resultHandler, selfDescriptionResponse, brokerFuture);
    }

    public void queryMessage(URI uri, Handler<AsyncResult<String>> resultHandler) {
       LOGGER.info("Yeah");
    }

    private void handleBrokerSelfDescription(Handler<AsyncResult<String>> resultHandler, SelfDescriptionResponse selfDescriptionResponse, Future<Broker> brokerFuture) {
        handleBrokerFuture(brokerFuture, contentBodyAsyncResult -> {
            if (contentBodyAsyncResult.succeeded()) {
                ContentBody contentBody = new StringBody(Json.encodePrettily(selfDescriptionResponse), ContentType.create("application/json"));
                ContentBody payload = contentBodyAsyncResult.result();
                MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
                        .setBoundary("IDSMSGPART")
                        .setCharset(StandardCharsets.UTF_8)
                        .setContentType(ContentType.APPLICATION_JSON)
                        .addPart("header", contentBody)
                        .addPart("payload", payload);

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try {
                    multipartEntityBuilder.build().writeTo(out);
                } catch (IOException e) {
                    LOGGER.error(e);
                    resultHandler.handle(Future.failedFuture(e.getMessage()));
                }
                resultHandler.handle(Future.succeededFuture(out.toString()));
            } else {
                resultHandler.handle(Future.failedFuture(contentBodyAsyncResult.cause()));
            }
        });
    }

    public void handleBrokerFuture(Future<Broker> brokerFuture, Handler<AsyncResult<ContentBody>> resultHandler) {
        brokerFuture.setHandler(brokerAsyncResult -> {
            if (brokerAsyncResult.succeeded()) {
                ContentBody payload = new StringBody(Json.encodePrettily(brokerAsyncResult.result()), ContentType.create("application/json"));
                resultHandler.handle(Future.succeededFuture(payload));
            } else {
                resultHandler.handle(Future.failedFuture(brokerAsyncResult.cause()));
            }
        });

    }

    public void update(URI uri, Connector connector, Handler<AsyncResult<String>> readyHandler) {
        resolveCatalogueId(connector.getId().toString(), next -> {
            if (next.succeeded()) {
                Future<String> catalogueFuture = Future.future();
                Map<String, Future<String>> datassetFutures = new HashMap<>();
                initTransformations(connector, catalogueFuture, datassetFutures);
                catalogueFuture.setHandler(reply -> {
                    handleCatalogueExternal(reply, next.result(), next2 -> {
                        updateDatasets(uri, datassetFutures, next.result(), readyHandler);
                    });
                });
            } else {
                LOGGER.error(next.cause());
                handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
            }
        });
    }

    public void register(URI uri, Connector connector, Handler<AsyncResult<String>> readyHandler) {
        String catalogueId = UUID.randomUUID().toString();
        Future<String> catalogueFuture = Future.future();
        Map<String, Future<String>> datassetFutures = new HashMap<>();
        initTransformations(connector, catalogueFuture, datassetFutures);
        catalogueFuture.setHandler(catalogueTTLResult ->
                handleCatalogueExternal(catalogueTTLResult, catalogueId, piveauCatalogueReply ->
                        handleCatalogueInternal(piveauCatalogueReply, connector.getId().toString(), internalCatalogueReply ->
                                handleDatasets(uri, internalCatalogueReply, datassetFutures, readyHandler))));
    }

    public void unregister(URI uri, Connector connector, Handler<AsyncResult<String>> readyHandler) {
        Map<String, Future> datasetDeleteFutures = new HashMap<>();

        resolveCatalogueId(connector.getId().toString(), catalogueIdResult -> {
            if (catalogueIdResult.succeeded()) {
                dataAssetIdsOfCatalogue(catalogueIdResult.result(), piveauDatasetIds -> {
                    if (piveauDatasetIds.succeeded()) {
                        if (!piveauDatasetIds.result().isEmpty()) {
                            for (String id : piveauDatasetIds.result()) {
                                Future datasetDeleteFuture = Future.future();
                                datasetDeleteFutures.put(id, datasetDeleteFuture);
                                deleteDatasetPiveau(id, catalogueIdResult.result(), next -> deleteDatasetInDatabase(next, DELETE_DS_UPDATE, id, datasetDeleteFutures));
                            }
                        }
                        for (Resource dataasset : connector.getCatalog().getOffer()) {
                            Future datasetDeleteFuture = Future.future();
                            datasetDeleteFutures.put(dataasset.getId().toString(), datasetDeleteFuture);
                            databaseService.query(SELECT_DS_STATEMENT, new JsonArray().add(dataasset.getId().toString()), datasetIdreply -> {
                                if (datasetIdreply.succeeded()) {
                                    if (!datasetIdreply.result().isEmpty()) {
                                        String datasePiveautId = datasetIdreply.result().get(0).getString("internal_id");
                                        String datasetIdsId = datasetIdreply.result().get(0).getString("external_id");
                                        deleteDatasetPiveau(datasePiveautId, catalogueIdResult.result(), externalDeleteReply ->
                                                deleteDatasetInternal(externalDeleteReply, datasePiveautId, datasetIdsId, datasetDeleteFutures));
                                    } else {
                                        datasetDeleteFuture.complete();
                                    }
                                } else {
                                    LOGGER.error(datasetIdreply.cause());
                                    handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                                }
                            });
                        }
                        handleCatalogue(uri, new ArrayList<>(datasetDeleteFutures.values()), catalogueIdResult.result(), readyHandler);
                    } else {
                        LOGGER.error(piveauDatasetIds.cause());
                        handleRejectionMessage(RejectionReason.NOT_FOUND, uri, readyHandler);
                    }
                });
            } else {
                handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
            }
        });
    }

    private void updateDataset(Map<String, Future<String>> datassetFutures, String messageId, String catalogueId, Map<String, Future> datasetUpdateFutures) {
        resolveDatasetsForUpdate(messageId, reply -> {
            brokerMessageService.createDataSet(datassetFutures.get(messageId).result(), reply.result(), catalogueId, datasetReply -> {
                if (datasetReply.succeeded()) {
                    datasetUpdateFutures.get(messageId).complete();
                } else {
                    LOGGER.error(datasetReply.cause());
                    datasetUpdateFutures.get(messageId).fail(datasetReply.cause());
                }
            });
        });
    }

    private void handleCatalogueExternal(AsyncResult<String> catalogue, String catalogueId, Handler<AsyncResult<String>> next) {
        if (catalogue.succeeded()) {
            brokerMessageService.createCatalogue(catalogue.result(), catalogueId, catalogueReply -> {
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

    private void handleCatalogueInternal(AsyncResult<String> reply, String connectorId, Handler<AsyncResult<String>> next) {
        if (reply.succeeded()) {
            databaseService.update(INSERT_CAT_STATEMENT, new JsonArray().add(connectorId).add(reply.result()), cataloguePersistenceReply -> {
                if (cataloguePersistenceReply.succeeded()) {
                    next.handle(Future.succeededFuture(reply.result()));
                } else {
                    LOGGER.error(cataloguePersistenceReply.cause());
                    next.handle(Future.failedFuture(cataloguePersistenceReply.cause()));
                }
            });
        } else {
            next.handle(Future.failedFuture(reply.cause()));
        }
    }

    private void updateDatasets(URI uri, Map<String, Future<String>> datassetFutures, String catalogueId, Handler<AsyncResult<String>> readyHandler) {
        Set<String> messageDatasetIds = datassetFutures.keySet();

        dataAssetIdsOfCatalogue(catalogueId, piveauDatasetIds -> resolvePiveauIds(piveauDatasetIds, result -> {
            if (result.succeeded()) {
                Map<String, Future> datasetUpdateFutures = new HashMap<>();
                Set<String> availableDatasetIds = new HashSet(result.result().keySet());
                availableDatasetIds.addAll(messageDatasetIds);
                for (String messageDatasetId : availableDatasetIds) {
                    datasetUpdateFutures.put(messageDatasetId, Future.future());
                }
                Collection<String> piveauIds = result.result().keySet();
                if (piveauIds.isEmpty() && messageDatasetIds.isEmpty()) {
                    handleSucceededMessage(uri, readyHandler);
                } else {
                    for (String messageId : messageDatasetIds) {
                        if (piveauIds.contains(messageId)) {
                            updateDataset(datassetFutures, messageId, catalogueId, datasetUpdateFutures);
                        } else {
                            String internalId = UUID.randomUUID().toString();
                            createDataSet(datassetFutures, messageId, internalId, catalogueId, datasetUpdateFutures);
                        }
                        piveauIds.remove(messageId);
                    }
                    for (String orphan : piveauIds) {
                        deleteDatasetPiveau(result.result().get(orphan), catalogueId, res -> deleteDatasetInDatabase(res, "DELETE FROM datasets WHERE external_id = ?", orphan, datasetUpdateFutures));
                    }
                    CompositeFuture.all(new ArrayList<>(datasetUpdateFutures.values())).setHandler(ac -> {
                        if (ac.succeeded()) {
                            handleSucceededMessage(uri, readyHandler);
                        } else {
                            handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                        }
                    });
                }
            } else {
                handleRejectionMessage(RejectionReason.NOT_FOUND, uri, readyHandler);
            }
        }));

    }

    private void handleDatasets(URI uri, AsyncResult<String> catalogueIdResult, java.util.Map<String, Future<String>> datassetFutures, Handler<AsyncResult<String>> readyHandler) {
        if (catalogueIdResult.succeeded()) {
            CompositeFuture.all(new ArrayList<>(datassetFutures.values())).setHandler(dataassetCreateReply -> {
                if (dataassetCreateReply.succeeded()) {
                    Map<String, Future> dataassetCreateFutures = new HashMap<>();
                    for (String datasetExternalId : datassetFutures.keySet()) {
                        dataassetCreateFutures.put(datasetExternalId, Future.future());
                        String datasetId = UUID.randomUUID().toString();
                        createDataSet(datassetFutures, datasetExternalId, datasetId, catalogueIdResult.result(), dataassetCreateFutures);
                    }
                    CompositeFuture.all(new ArrayList<>(dataassetCreateFutures.values())).setHandler(ac -> {
                        if (ac.succeeded()) {
                            handleSucceededMessage(uri, readyHandler);
                        } else {
                            LOGGER.error(ac.cause());
                            handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                        }
                    });
                } else {
                    LOGGER.error(dataassetCreateReply.cause());
                    handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                }
            });
        } else {
            handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
        }
    }

    private void handleCatalogue(URI uri, List<Future> datasetDeleteFutures, String catalogueIdResult, Handler<AsyncResult<String>> readyHandler) {
        CompositeFuture.all(datasetDeleteFutures).setHandler(reply -> {
            if (reply.succeeded()) {
                deleteCatalogueExternal(reply, catalogueIdResult, externalCatalogueDeleteReply ->
                        deleteCatalogueInternal(uri, externalCatalogueDeleteReply, catalogueIdResult, readyHandler));
            } else {
                handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
            }
        });
    }

    private void resolveCatalogueId(String connectorId, Handler<AsyncResult<String>> next) {
        databaseService.query(SELECT_CAT_STATEMENT, new JsonArray().add(connectorId), cataloguePersistenceReply -> {
            if (cataloguePersistenceReply.succeeded() && !cataloguePersistenceReply.result().isEmpty()) {
                LOGGER.info("internal ID resolved: " + cataloguePersistenceReply.result().get(0).getString("internal_id"));
                next.handle(Future.succeededFuture(cataloguePersistenceReply.result().get(0).getString("internal_id")));
            } else {
                LOGGER.error(cataloguePersistenceReply.cause());
                next.handle(Future.failedFuture(cataloguePersistenceReply.cause()));
            }
        });
    }

    private void resolveDatasetsForUpdate(String dataassetIdExternal, Handler<AsyncResult<String>> next) {
        databaseService.query(SELECT_DS_STATEMENT, new JsonArray().add(dataassetIdExternal), datasetPersistenceReply -> {
            if (datasetPersistenceReply.succeeded()) {
                if (!datasetPersistenceReply.result().isEmpty()) {
                    String id = datasetPersistenceReply.result().get(0).getString("internal_id");
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

    private void deleteDatasetInDatabase(AsyncResult<Void> reply, String update, String idsId, Map<String, Future> datasetdeleteFutures) {
        if (reply.succeeded()) {
            databaseService.update(update, new JsonArray().add(idsId), internalDatasetDeleteResult -> {
                if (reply.succeeded()) {
                    datasetdeleteFutures.get(idsId).complete();
                    LOGGER.info("DataAsset From Database successfully deleted");
                } else {
                    LOGGER.error(reply.cause());
                    datasetdeleteFutures.get(idsId).fail(reply.cause());
                }
            });
        } else {
            LOGGER.error("DataAsset delete failed");
        }
    }

    private void resolvePiveauIds(AsyncResult<List<String>> piveauDatasetIds, Handler<AsyncResult<Map<String, String>>> completer) {
        Map<String, Future<List<JsonObject>>> piveau2IDSResolveFutureMap = new HashMap<>();
        if (piveauDatasetIds.succeeded()) {
            for (String piveauId : piveauDatasetIds.result()) {
                Future idsResolve = Future.future();
                piveau2IDSResolveFutureMap.put(piveauId, idsResolve);
                databaseService.query(RESOLVE_DS_STATEMENT, new JsonArray().add(piveauId), idsResolve.completer());
            }
            CompositeFuture.all(new ArrayList<>(piveau2IDSResolveFutureMap.values())).setHandler(ac -> {
                if (ac.succeeded()) {
                    Map<String, String> resultMap = new HashMap<>();
                    for (String piveauId : piveau2IDSResolveFutureMap.keySet()) {
                        resultMap.put(piveau2IDSResolveFutureMap.get(piveauId).result().get(0).getString("external_id"), piveauId);
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

    private void dataAssetIdsOfCatalogue(String catalogueInternalId, Handler<AsyncResult<List<String>>> asyncResultHandler) {
        brokerMessageService.getAllDatasetsOfCatalogue(catalogueInternalId, jsonReply -> {
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

    private void createDataSet(Map<String, Future<String>> datassetFutures, String datasetExternalId, String dataSetId, String catalogueId, Map<String, Future> datasetUpdateFutures) {
        brokerMessageService.createDataSet(datassetFutures.get(datasetExternalId).result(), dataSetId, catalogueId, datasetReply -> {
            if (datasetReply.succeeded()) {
                databaseService.update(INSERT_DS_STATEMENT, new JsonArray().add(datasetExternalId).add(dataSetId), datasetPersistenceReply2 -> {
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

    private void deleteDatasetPiveau(String datasetId, String catalogueId, Handler<AsyncResult<Void>> next) {
        brokerMessageService.deleteDataSet(datasetId, catalogueId, deleteAsset -> {
            if (deleteAsset.succeeded()) {
                next.handle(Future.succeededFuture());
            } else {
                LOGGER.error(deleteAsset.cause());
                next.handle(Future.failedFuture(deleteAsset.cause()));
            }
        });
    }

    private void deleteDatasetInternal(AsyncResult<Void> reply, String datasetPiveauId, String datasetIDSId, Map<String, Future> datasetDeleteFutures) {
        if (reply.succeeded()) {
            databaseService.update(DELETE_DS_UPDATE, new JsonArray().add(datasetPiveauId), internalDatasetDeleteResult -> {
                if (reply.succeeded()) {
                    LOGGER.info("DataAsset From Database successfully deleted");
                    datasetDeleteFutures.get(datasetIDSId).complete();
                } else {
                    datasetDeleteFutures.get(datasetIDSId).fail(reply.cause());
                    LOGGER.error(reply.cause());
                }
            });
        } else {
            datasetDeleteFutures.get(datasetPiveauId).fail(reply.cause());
        }
    }

    private void deleteCatalogueExternal(AsyncResult<CompositeFuture> reply, String catalogueInternalId, Handler<AsyncResult> next) {
        if (reply.succeeded()) {
            brokerMessageService.deleteCatalogue(catalogueInternalId, deleteCatalogueReply -> {
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
            databaseService.update(DELETE_CAT_STATEMENT, new JsonArray().add(catalogueInternalId), deleteCatalogueReply -> {
                if (deleteCatalogueReply.succeeded()) {
                    LOGGER.info("Catalogue From Database succeeded deleted");
                    handleSucceededMessage(uri, readyHandler);
                } else {
                    LOGGER.error(deleteCatalogueReply.cause());
                    handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                }
            });
        } else {
            LOGGER.error(reply.cause());
            handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
        }
    }

    private void initTransformations(Connector connector, Future<String> catalogueFuture, Map<String, Future<String>> datassetFutures) {
        String con = Json.encode(connector);
        dcatTransformerService.transformCatalogue(con, catalogueFuture.completer());
        if (connector.getCatalog() != null) {
            for (Resource resource : connector.getCatalog().getOffer()) {
                Future<String> dataassetFuture = Future.future();
                datassetFutures.put(resource.getId().toString(), dataassetFuture);
                dcatTransformerService.transformDataset(Json.encode(resource), dataassetFuture.completer());
            }
        }
    }

    private void createSucceededMessage(URI uriOfHeader, Handler<AsyncResult<MessageProcessedNotification>> resultHandler) {
        try {
            String uuid = UUID.randomUUID().toString();
            MessageProcessedNotification message = new MessageProcessedNotificationBuilder(new URI(uuid))
                    ._correlationMessage_(uriOfHeader)
                    ._issued_(getDate())
                    ._modelVersion_("2.0.0")
                    ._issuerConnector_(new URI("URI"))
                    ._securityToken_(new DynamicAttributeTokenBuilder()
                            ._tokenFormat_(TokenFormat.JWT)
                            ._tokenValue_(getJWT())
                            .build())
                    .build();
            resultHandler.handle(Future.succeededFuture(message));
        } catch (URISyntaxException e) {
            LOGGER.error(e);
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    private void createRejectionMessage(RejectionReason rejectionReason, URI uriOfHeader, Handler<AsyncResult<RejectionMessage>> resultHandler) {
        try {
            String uuid = UUID.randomUUID().toString();
            RejectionMessage message = new RejectionMessageBuilder(new URI(uuid))
                    ._correlationMessage_(uriOfHeader)
                    ._issued_(getDate())
                    ._modelVersion_("2.0.0")
                    ._issuerConnector_(new URI("URI"))
                    ._securityToken_(new DynamicAttributeTokenBuilder()
                            ._tokenFormat_(TokenFormat.JWT)
                            ._tokenValue_(getJWT())
                            .build())
                    ._rejectionReason_(rejectionReason)
                    .build();
            resultHandler.handle(Future.succeededFuture(message));
        } catch (URISyntaxException e) {
            LOGGER.error(e);
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    private XMLGregorianCalendar getDate() {
        GregorianCalendar c = new GregorianCalendar();
        c.setTime(new Date());
        try {
            return DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
        } catch (DatatypeConfigurationException e) {
            LOGGER.error(e);
        }
        return null;
    }

    private String getJWT() {
        return "abcdefg12";
    }

    private Buffer createMultipartMessage(Message message, Connector connector) {
        ContentBody cb = new StringBody(Json.encodePrettily(message), org.apache.http.entity.ContentType.create("application/json"));
        ContentBody result = new StringBody(Json.encodePrettily(connector), org.apache.http.entity.ContentType.create("application/json"));

        MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
                .setBoundary("IDSMSGPART")
                .setCharset(StandardCharsets.UTF_8)
                .setContentType(ContentType.APPLICATION_JSON)
                .addPart("header", cb);
        //.addPart("payload", result);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            multipartEntityBuilder.build().writeTo(out);
            return Buffer.buffer().appendString(out.toString());
        } catch (IOException e) {
            LOGGER.error(e);
        }
        return null;
    }

    private void handleSucceededMessage(URI uri, Handler<AsyncResult<String>> readyHandler) {
        createSucceededMessage(uri, messageProcessedNotificationAsyncResult -> {
            if (messageProcessedNotificationAsyncResult.succeeded()) {
                Buffer buffer = createMultipartMessage(messageProcessedNotificationAsyncResult.result(), null);
                readyHandler.handle(Future.succeededFuture(buffer.toString()));
            } else {
                readyHandler.handle(Future.failedFuture(messageProcessedNotificationAsyncResult.cause()));
            }
        });
    }

    public void handleRejectionMessage(RejectionReason rejectionReason, URI uri, Handler<AsyncResult<String>> readyHandler) {
        createRejectionMessage(rejectionReason, uri, rejectionMessageAsyncResult -> {
            if (rejectionMessageAsyncResult.succeeded()) {
                Buffer buffer = createMultipartMessage(rejectionMessageAsyncResult.result(), null);
                readyHandler.handle(Future.succeededFuture(buffer.toString()));
            } else {
                readyHandler.handle(Future.failedFuture(rejectionMessageAsyncResult.cause()));
            }
        });
    }

    public SelfDescriptionResponse buildSelfDescriptionResponse(JsonObject jsonObject) {

        try {
            return new SelfDescriptionResponseBuilder(new URI("broker#SelfDescriptionResponse"))
                    ._issued_(getDate())
                    ._modelVersion_(INFO_MODEL_VERSION)
                    .build();
        } catch (URISyntaxException e) {
            LOGGER.error(e);
        }
        return null;
    }

    public void buildBroker(JsonObject config, Handler<AsyncResult<Broker>> next) {
        Future<ArrayList<URI>> listFutureExternalIds = Future.future();
        listOfExternalIds(listFutureExternalIds.completer());
        listFutureExternalIds.setHandler(arrayListAsyncResult -> {
            try {
                BrokerBuilder brokerBuilder = new BrokerBuilder((new URI("payload#Broker")))
                        ._maintainer_(new URI("maintainer"))
                        ._version_("0.0.1")
                        ._curator_(new URI("curator"))
                        ._connector_(listFutureExternalIds.result())
                        ._outboundModelVersion_(INFO_MODEL_VERSION)
                        ._inboundModelVersion_(new ArrayList<>(Arrays.asList(SUPPORTED_INFO_MODEL_VERSIONS)));
                next.handle(Future.succeededFuture(brokerBuilder.build()));
            } catch (Exception e) {
                LOGGER.error(e);
                next.handle(Future.failedFuture(e.getMessage()));
            }
        });
    }

    private void listOfExternalIds(Handler<AsyncResult<ArrayList<URI>>> next) {
        ArrayList<URI> externalIds = new ArrayList<>();
        databaseService.query("SELECT * FROM catalogues", new JsonArray(), data -> {
            if (data.succeeded()) {
                for (JsonObject jsonObject : data.result()) {
                    try {
                        externalIds.add(new URI(jsonObject.getString("external_id")));
                    } catch (URISyntaxException e) {
                        e.printStackTrace();
                    }
                }
                next.handle(Future.succeededFuture(externalIds));
            } else {
                LOGGER.error(data.cause());
                next.handle(Future.failedFuture(data.cause()));
            }
        });
    }

}
