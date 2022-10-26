package com.bnpparibas.gban.e2etests.downstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.bnpparibas.gba.consolatio.flow.handlers.AbstractSolaceSendHandler;
import com.bnpparibas.gba.consolatio.marshaller.StringMarshaller;
import com.bnpparibas.gba.consolatio.transport.PiblishException;
import com.bnpparibas.gba.consolatio.transport.SolaceMessage;

@Component
public class TigerCSVEmulator extends AbstractSolaceSendHandler<String> {

    private static final Logger logger = LoggerFactory.getLogger(TigerCSVEmulator.class);

    @Value("${consolatio.egressTopic")
    String egressTopic;

    @Bean
    static StringMarshaller solaceUnmarshaller() {
        return new StringMarshaller();
    }

    @Override
    public void onMessage(String bookRequest, SolaceMessage solaceMessage) {
        logger.info("Received {} from message from UsStreetProcessor", bookRequest);

        try {
            String[] bookRequestParts = bookRequest.split(",");

            String correlationId = bookRequestParts[37];

            String reply = String.join(",", "100", correlationId, "20220603-11:05:12");

            sendSolaceMessage(egressTopic, reply);
        } catch (PublishException e) {
            throw new RuntimeException("Failed to send reply", e);
        }
    }
}