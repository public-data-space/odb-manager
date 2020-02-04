package de.fraunhofer.fokus.ids.utils;

import de.fraunhofer.iais.eis.Connector;
import de.fraunhofer.iais.eis.ConnectorNotificationMessage;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class IDSMessageParser {
    private static Logger LOGGER = LoggerFactory.getLogger(IDSMessageParser.class.getName());

    private static final String SEPARATOR = "IDSMSGPART";

    public static ConnectorNotificationMessage getHeader (String input){
        try{
            int beginBody = input.indexOf("--"+SEPARATOR, ("--"+SEPARATOR).length()+1);
            String headerPart = input.substring(0, beginBody);

            String header  = headerPart.substring(headerPart.indexOf("{"),headerPart.lastIndexOf("}")+1);
            return Json.decodeValue(header, ConnectorNotificationMessage.class);
        }
        catch(Exception e) {
            LOGGER.error(e);
            return null;
        }
    }

    public static Connector getBody (String input){
        try{
            int beginBody = input.indexOf("--"+SEPARATOR, ("--"+SEPARATOR).length()+1);
            String bodyPart = input.substring(beginBody);
            String body  = bodyPart.substring(bodyPart.indexOf("{"),bodyPart.lastIndexOf("}")+1);
            return Json.decodeValue(body, Connector.class);
        }
        catch(Exception e) {
            LOGGER.error(e);
            return null;
        }
    }
}
