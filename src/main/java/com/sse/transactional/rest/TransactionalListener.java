package com.sse.transactional.rest;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import jakarta.ejb.Singleton;
import jakarta.ejb.Startup;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

@Singleton
@Startup
public class TransactionalListener {
    private static final String TRANSACTION_TOPIC = "transactions";

    @Resource
    TransactionSynchronizationRegistry transactionSynchronizationRegistry;

    private KafkaProducer<String, String> transactionProducer;

    private final Set<Object> registeredSynchronizations = Collections.synchronizedSet(new HashSet<>());

    @PostConstruct
    void init() {
        Properties p = new Properties();
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("kafka.properties")) {
            p.load(stream);
            transactionProducer = new KafkaProducer<>(p, new StringSerializer(), new StringSerializer());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int counter() {
        return registeredSynchronizations.size();
    }

    public void createSynchronization(Object transactionKey) {
        if (registeredSynchronizations.contains(transactionKey)) {
            return;
        }

        registeredSynchronizations.add(transactionKey);
        transactionSynchronizationRegistry.registerInterposedSynchronization(new Synchronizer(
                transactionKey, transactionProducer, registeredSynchronizations::remove
        ));
    }


    public static class Synchronizer implements Synchronization {
        private final Object transactionKey;
        private final KafkaProducer<String, String> transactionProducer;

        private final Consumer<Object> remover;

        public Synchronizer(Object transactionKey, KafkaProducer<String, String> transactionProducer, Consumer<Object> remover) {
            this.transactionKey = transactionKey;
            this.transactionProducer = transactionProducer;
            this.remover = remover;
        }

        @Override
        public void beforeCompletion() {
        }

        @Override
        public void afterCompletion(int status) {
            transactionProducer.send(new ProducerRecord<>(TRANSACTION_TOPIC, transactionKey.toString(), Integer.toString(status)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        System.err.println("Could not send the transaction status");
                        e.printStackTrace(System.err);
                    }

                    if (recordMetadata != null) {
                        System.out.println("Wrote transaction information " + status + " to " + recordMetadata.topic() + " partition " + recordMetadata.partition() + " offset " + recordMetadata.offset());
                    }
                }
            });
            remover.accept(transactionKey);
        }
    }
}
