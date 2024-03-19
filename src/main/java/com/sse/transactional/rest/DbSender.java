package com.sse.transactional.rest;

import com.github.javafaker.Faker;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import jakarta.ejb.EJB;
import jakarta.ejb.Singleton;
import jakarta.ejb.TransactionAttribute;
import jakarta.transaction.TransactionSynchronizationRegistry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static jakarta.ejb.TransactionAttributeType.REQUIRED;

@Singleton
public class DbSender {
    private static final String REAL_TOPIC = "messages";
    @Resource
    private DataSource dataSource;

    @EJB
    private TransactionalListener transactionalListener;

    @Resource
    TransactionSynchronizationRegistry transactionSynchronizationRegistry;

    private KafkaProducer<String, String> producer;

    @PostConstruct
    void init() {
        Properties p = new Properties();
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("kafka.properties")) {
            p.load(stream);
            producer = new KafkaProducer<>(p, new StringSerializer(), new StringSerializer());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @TransactionAttribute(REQUIRED)
    public List<RecordInfo> execute() {
        System.out.println("Key: " + transactionSynchronizationRegistry.getTransactionKey());
        Object transactionKey = transactionSynchronizationRegistry.getTransactionKey();
        transactionalListener.createSynchronization(transactionKey);

        try (Connection c = dataSource.getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("select kafka from queue")) {
            ArrayList<Future<RecordMetadata>> futures = new ArrayList<>();
            while (rs.next()) {
                futures.add(producer.send(new ProducerRecord<>(REAL_TOPIC, null, rs.getString(1))));
            }
            return futures.stream()
                    .map(f -> {
                        try {
                            return f.get(3, TimeUnit.SECONDS);
                        } catch (InterruptedException | ExecutionException | TimeoutException e) {
                            e.printStackTrace(System.err);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .map(r -> new RecordInfo(r.topic(), r.partition(), r.offset()))
                    .collect(Collectors.toList());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @TransactionAttribute(REQUIRED)
    public String insert() {
        String fact = Faker.instance().chuckNorris().fact();
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement("insert into queue(kafka) values (?)")) {
            ps.setString(1, fact);
            ps.execute();
            return fact;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
