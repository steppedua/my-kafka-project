package api.config;

import api.domain.Payment;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.util.Strings;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Configuration
@EnableKafka
public class MyConsumerConfiguration {

    private final KafkaTemplate<String, Payment> kafkaTemplate;

    public MyConsumerConfiguration(KafkaTemplate<String, Payment> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Bean
    public ConsumerFactory<String, Payment> consumerFactory() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // key.deserializer
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // value.deserializer
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // spring.deserializer.key.delegate.class
        properties.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, JsonDeserializer.class);

        ErrorHandlingDeserializer<Payment> errorHandlingDeserializer
                = new ErrorHandlingDeserializer<>(new JsonDeserializer<>(Payment.class));

        return new DefaultKafkaConsumerFactory<>(
                properties,
                new StringDeserializer(),
                errorHandlingDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Payment> kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, Payment> concurrentKafkaListenerContainerFactory
                = new ConcurrentKafkaListenerContainerFactory<>();
        concurrentKafkaListenerContainerFactory.setConsumerFactory(consumerFactory());

        // Ретрай сообщения (сколько раз ретраить и через какой промежуток времени ретраить)
        concurrentKafkaListenerContainerFactory.setRetryTemplate(kafkaRetry());

        // Что делать на каждый ретрай
        concurrentKafkaListenerContainerFactory.setRecoveryCallback(this::retryOption1);

        concurrentKafkaListenerContainerFactory.setErrorHandler(new SeekToCurrentErrorHandler(
                new DeadLetterPublishingRecoverer(kafkaTemplate),
                // new ExponentialBackOff(100, 1.3D)
                new FixedBackOff(3000, 3))
        );

        return concurrentKafkaListenerContainerFactory;
    }


    private Object retryOption1(RetryContext retryContext) {
        // На каждый ретрай выводим его в лог
        ConsumerRecord<String, Payment> consumerRecord = (ConsumerRecord) retryContext.getAttribute("record");
        Payment value = consumerRecord.value();
        log.info("Recovery is called for message {} ", value);

        // На последний ретрай отправляем сообщение в DLQ
        // RetryContext.EXHAUSTED показывает, что количество ретраев закончилось
        // и на последнем ретрае отправляем сообщение в DLQ
        if (Boolean.TRUE.equals(retryContext.getAttribute(RetryContext.EXHAUSTED))) {
            log.info("MOVED TO ERROR DLQ");
            // Устанавливаем ErrorMessage, который есть в Payment
            value.setErrorMessage(getThrowableSafely(retryContext));

            kafkaTemplate.send(
                    "test.dlq",
                    consumerRecord.key(),
                    consumerRecord.value()
            );
        }

        return Optional.empty();
    }

    private String getThrowableSafely(RetryContext retryContext) {
        Throwable lastThrowable = retryContext.getLastThrowable();

        if (lastThrowable == null) {
            return Strings.EMPTY;
        }

        return lastThrowable.getMessage();
    }

    private RetryTemplate kafkaRetry() {
        RetryTemplate retryTemplate = new RetryTemplate();

        // Промежуток времени между каждым ретраем (сколько спим между каждым ретраем)
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();  // other policies are not better
        fixedBackOffPolicy.setBackOffPeriod(3 * 1000L);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);

        // Сколько ретраев всего делать
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(); // other policies are not better
        retryPolicy.setMaxAttempts(3);
        retryTemplate.setRetryPolicy(retryPolicy);

        return retryTemplate;
    }
}
