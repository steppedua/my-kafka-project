package api.consumer;

import api.domain.Payment;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumer {
    private final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    // TODO это из статьи на хабре
//    @KafkaListener(topics = "transaction-1")
//    public void listener(final @Payload String message,
//                        final @Header(KafkaHeaders.OFFSET) Integer offset,
//                        final @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
//                        final @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
//                        final @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
//                        final @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts,
//                        final Acknowledgment acknowledgment
//    ) {
//        logger.info(String.format("#### -> Consumed message -> TIMESTAMP: %d\n%s\noffset: %d\nkey: %s\npartition: %d\ntopic: %s", ts, message, offset, key, partition, topic));
//        acknowledgment.acknowledge();
//    }

    // TODO это из курса индуса
//    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
//    private final MyConsumerConfig myConsumerConfig;
//
//    @Autowired
//    public KafkaConsumer(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry, MyConsumerConfig myConsumerConfig) {
//        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
//        this.myConsumerConfig = myConsumerConfig;
//    }
//
//    @EventListener
//    public void onAppStarted(ApplicationStartedEvent event) {
//        kafkaAdminClient.checkTopicsCreated();
//        LOG.info("Topics with name {} is ready for operations!", kafkaConfig.getTopicNamesToCreate().toArray());
//        kafkaListenerEndpointRegistry.getListenerContainer("twitterAnalyticsTopicListener").start();
//    }

//    // TODO это пример отсюда
//    @KafkaListener(topics = "transaction-1")
//    public void listener(@Payload Payment account,
//                         ConsumerRecord<String, Payment> cr) {
//        logger.info("Topic [transaction-1] Received message from {} with {} PLN ", account.getName(), account.getAmount());
//        logger.info(cr.toString());
//    }

    // TODO это пример из Spring Kafka Consuming Strategies
    @KafkaListener(topics = "transaction-1", groupId = "group-1")
    public void processPayment(@Payload Payment payment, ConsumerRecord<String, Payment> cr) {
        //Если сумма (Amount) отрицательная, то надо 3 раза делать Retry
        log.debug(">>> Payment process started, idempotencyKey={}", payment.getIdempotencyKey());
        if (Double.parseDouble(payment.getAmount().toString()) > 0.0) {
            log.error("Amount can't be negative, found in Payment");
            throw new RuntimeException("Amount can't be negative, found in Payment=" + payment);
        }
        log.debug("<<< Payment processed: {}", payment);
        log.debug(cr.toString());
    }

//    @KafkaListener(topics = "transaction-1", groupId = "group-1")
//    public void listener(@Payload Payment payment, ConsumerRecord<String, Payment> cr) {
//        logger.info("Topic [transaction-1] Received message from {}", payment.toString());
//        logger.info(cr.toString());
//    }
}
