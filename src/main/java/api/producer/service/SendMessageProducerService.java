package api.producer.service;

import api.domain.Payment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class SendMessageProducerService {
    private final Logger logger = LoggerFactory.getLogger(SendMessageProducerService.class);
    private final KafkaTemplate<String, Payment> kafkaTemplate;

    public SendMessageProducerService(KafkaTemplate<String, Payment> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(@RequestBody final Payment payment) {
        ListenableFuture<SendResult<String, Payment>> listenableFuture = kafkaTemplate.send(
                "transaction-1",
                new Payment(
                        payment.getIdempotencyKey(),
                        payment.getAmount(),
                        payment.getInitiatedOn(),
                        payment.getErrorMessage()
                ));

        try {
            listenableFuture.get(10000, TimeUnit.MINUTES);
            logger.info("Сообщение доставлено!");
        } catch (InterruptedException e) {
            logger.warn("await interrupted", e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException("Unable to send message in kafka", e.getCause());
            }
        } catch (TimeoutException e) {
            logger.error("Сообщение доставлялось более, чем 10 секунд");
            throw new RuntimeException(e);
        }

    }

}
