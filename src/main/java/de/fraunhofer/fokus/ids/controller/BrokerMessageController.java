package de.fraunhofer.fokus.ids.controller;

import de.fraunhofer.fokus.ids.services.brokerMessageService.BrokerMessageService;
import de.fraunhofer.fokus.ids.utils.IDSMessageParser;
import de.fraunhofer.iais.eis.*;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.UUID;

public class BrokerMessageController {
    private Logger LOGGER = LoggerFactory.getLogger(BrokerMessageController.class.getName());
    private BrokerMessageService brokerMessageService;

    public BrokerMessageController(Vertx vertx) {
        this.brokerMessageService = BrokerMessageService.createProxy(vertx,"brokerMessageService");
    }

    public void getData (String input){
        ConnectorNotificationMessage header = IDSMessageParser.getHeader(input);
        Connector connector = IDSMessageParser.getBody(input);
        try {
            if (header instanceof ConnectorAvailableMessage) {
                register(connector);
            } else if (header instanceof ConnectorUnavailableMessage) {
                unregister(connector);
            } else if (header instanceof ConnectorUpdateMessage) {
                update(connector);
            } else {
                LOGGER.error("Invalid message signature.");
            }
        }
        catch (Exception e){
            LOGGER.error("Something went wrong while parsing the IDS message.");
        }
    }

    private void update(Connector connector) {
        System.out.println("Hier ist update");
    }

    private void register(Connector connector) {
        String catalogueid = UUID.randomUUID().toString();
        brokerMessageService.createCatalogue(toJson(connector),catalogueid, catalogueReply -> {
            if(catalogueReply.succeeded()){
                String datasetid = UUID.randomUUID().toString();
                brokerMessageService.createDataSet(toJson(connector), datasetid, catalogueid, datasetReply -> {
                    if(datasetReply.succeeded()){
                        LOGGER.info("success");
                    } else {
                        LOGGER.error(datasetReply.cause());
                    }
                });
            } else {
                LOGGER.error(catalogueReply.cause());
            }
        });
    }

    private void unregister(Connector connector) {
        String id = UUID.randomUUID().toString();
    //    brokerMessageService.sendBody(toJson(connector),id);
        System.out.println("Hier ist unregister");
    }


    private JsonObject toJson(Object object){
        try {
            return new JsonObject(Json.encode(object));
        } catch (Exception e){
            LOGGER.error("Something went wrong while parsing the IDS Connector.");
        }
        return null;
    }
}
