package api.producer.controller;

import api.domain.Payment;
import api.producer.service.SendMessageProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RequestMapping("/api/kafka")
@RestController
public class KafkaProducerController {
    private final Logger logger = LoggerFactory.getLogger(KafkaProducerController.class);
    private final SendMessageProducerService sendMessageProducerService;

    public KafkaProducerController(SendMessageProducerService sendMessageProducerService) {
        this.sendMessageProducerService = sendMessageProducerService;
    }

    @PostMapping
    public void sendMessage(@RequestBody Payment payment) {
        sendMessageProducerService.sendMessage(payment);

        logger.info("Message correct send");
    }
}
