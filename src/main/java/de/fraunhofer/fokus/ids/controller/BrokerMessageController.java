package de.fraunhofer.fokus.ids.controller;

import de.fraunhofer.fokus.ids.services.brokerMessageService.BrokerMessageService;
import de.fraunhofer.iais.eis.Connector;
import de.fraunhofer.iais.eis.ConnectorAvailableMessage;
import de.fraunhofer.iais.eis.ConnectorUnavailableMessage;
import de.fraunhofer.iais.eis.ConnectorUpdateMessage;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class BrokerMessageController {
    private Logger LOGGER = LoggerFactory.getLogger(BrokerMessageController.class.getName());
    private BrokerMessageService brokerMessageService;

    public BrokerMessageController(Vertx vertx) {
        this.brokerMessageService = BrokerMessageService.createProxy(vertx,"brokerMessageService");
    }

    public void getData (String input){
        String header = getHeader(input);
        String body = getBody(input);
        try {
            ConnectorAvailableMessage connectorAvailableMessage =  Json.decodeValue(header, ConnectorAvailableMessage.class);
            register(Json.decodeValue(body, Connector.class));
        } catch (DecodeException exception) {
            try {
                ConnectorUnavailableMessage connectorUnavailableMessage =  Json.decodeValue(header, ConnectorUnavailableMessage.class);
                unregister(Json.decodeValue(body, Connector.class));
            } catch (DecodeException exception2) {
                try {
                    ConnectorUpdateMessage connectorUpdateMessage =  Json.decodeValue(header, ConnectorUpdateMessage.class);
                    update(Json.decodeValue(body, Connector.class));
                }
                catch (DecodeException exception3){

                }
            }
        }
    }

    private void update(Connector body) {
        JsonObject jsonObject = (JsonObject) body;
        brokerMessageService.sendBody(jsonObject);
        System.out.println("Hier ist update");
    }

    private void register(Connector body) {
        System.out.println("Hier ist register");
    }

    private void unregister(Connector body) {
        System.out.println("Hier ist unregister");
    }

    private String getHeader (String input){
        JsonObject header = new JsonObject();
        String stringOfHeader  = input.substring(input.indexOf("{"),input.indexOf("}")+1);
        header.put("Header ",stringOfHeader);
        return stringOfHeader;
    }
    private String getBody (String input){
        JsonObject body = new JsonObject();
        String stringOfBody  = input.substring(input.indexOf("{", input.indexOf("{") + 1),input.lastIndexOf("}")+1);
        body.put("Body ",stringOfBody);
        return stringOfBody;
    }

}
