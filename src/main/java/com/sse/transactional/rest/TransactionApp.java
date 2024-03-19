package com.sse.transactional.rest;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.ejb.Singleton;
import jakarta.ejb.Startup;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

// no error handling or similar, just PoC
@Singleton
@Startup
public class TransactionApp {

    private KafkaStreams streams;

    @PostConstruct
    public void init() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> messages = builder.stream("messages", Consumed.with(new Serdes.StringSerde(), new Serdes.StringSerde()));

        // in the transactions we are only interested in the last message (because a transaction just has one final state)
        KStream<String, String> transactions = builder.stream("transactions", Consumed.with(new Serdes.StringSerde(), new Serdes.StringSerde()));

        // 3 is the transaction status for committed, constants are nice too ;)
        KStream<String, String> committed = transactions.filter((key, value) -> "3".equals(value));

        // no transaction was active when message arrived -> move forward
        messages.filter((k, v) -> k == null).to("transacted_messages");

        // there is a transaction key, so it needs a transaction, but need to check whether I need a time window for this one...
        messages.filter((k, v) -> k != null)
                .join(committed, (message, transactionCommitMessage) -> message,
                        JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(15), Duration.ofMinutes(10)),
                        StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()))
                .to("transacted_messages", Produced.with(new Serdes.StringSerde(), new Serdes.StringSerde()));

        Topology topology = builder.build();

        Properties p = new Properties();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("kafka_streams.properties")) {
            p.load(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        StreamsConfig config = new StreamsConfig(p);

        streams = new KafkaStreams(topology, config);
        streams.setUncaughtExceptionHandler(throwable -> {
            // TODO make it more logical but for now, we just ignore all errors and just restart the given threads - but could naturally fail brutally
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        streams.start();
    }

    @PreDestroy
    public void shutdown() {
        streams.close();
    }

    public KafkaStreams getStreams() {
        return streams;
    }
}
