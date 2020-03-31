package de.fraunhofer.fokus.ids.utils;

import de.fraunhofer.fokus.ids.models.IDSMessage;
import de.fraunhofer.iais.eis.*;
import de.fraunhofer.iais.eis.ids.jsonld.Serializer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.http.MultiPartFormInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Optional;

public class IDSMessageParser {
    private static Logger LOGGER = LoggerFactory.getLogger(IDSMessageParser.class.getName());
    private static final String SEPARATOR = "IDSMSGPART";
    private static Serializer serializer = new Serializer();

    public static Optional<IDSMessage> parse(String requestMessage){

        InputStream messageBodyStream = new ByteArrayInputStream(requestMessage.getBytes(Charset.defaultCharset()));

        MultiPartFormInputStream multiPartInputStream = new MultiPartFormInputStream(messageBodyStream, "multipart/form-data; boundary="+SEPARATOR, null, null);
        try {
            String headerString = IOUtils.toString(multiPartInputStream.getPart("header").getInputStream(),Charset.defaultCharset());
            String payloadString = IOUtils.toString(multiPartInputStream.getPart("payload").getInputStream(),Charset.defaultCharset());
            return Optional.of(new IDSMessage(serializer.deserialize(headerString, Message.class), payloadString));
        } catch (IOException e) {
            LOGGER.error(e);
        }
        return Optional.empty();
    }
}
