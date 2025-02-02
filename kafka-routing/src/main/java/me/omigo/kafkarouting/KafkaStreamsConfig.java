package me.omigo.kafkarouting;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean
    public Map<String, Integer> map() {
        return new HashMap<>();
    }

    @Bean
    public KStream<String, String> kafkaStream(StreamsBuilder streamsBuilder, Map<String, Integer> map) {
        String inputTopic = "words";
        String outputTopic = "counts";

        KStream<String, String> stream = streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        stream.process(() -> (Processor<String, String, Void, Void>) record -> record.headers().forEach(header -> {
            System.out.println("Header: " + header.key() + " = " + new String(header.value()));
        }));

        stream.mapValues(value -> {
            map.put(value, map.getOrDefault(value, 0) + 1);
            return value + " -> " + map.get(value);
        }).to(outputTopic);

        return stream;
    }
}
