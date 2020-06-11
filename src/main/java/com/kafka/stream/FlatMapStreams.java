package com.kafka.stream;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class FlatMapStreams {

	public static void main(String[] args) {
		// Set up the configuration.
		final Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "flatmap-stream");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		// Get the source stream.
		final StreamsBuilder builder = new StreamsBuilder();
		final KStream<String, String> source = builder.stream("streams-flatmap-input-topic");

		KStream<String, String> flatMapStream = source.flatMap((key, value) -> {
			 List<KeyValue<String, String>> result = new LinkedList<>();
	            result.add(KeyValue.pair(key, value.toUpperCase()));
	            result.add(KeyValue.pair(key, value.toLowerCase()));
	            return result;
		});

		flatMapStream.to("streams-flatmap-output-topic");

		final Topology topology = builder.build();
		final KafkaStreams streams = new KafkaStreams(topology, props);
		// Print the topology to the console.
		System.out.println(topology.describe());
		final CountDownLatch latch = new CountDownLatch(1);

		// Attach a shutdown handler to catch control-c and terminate the application
		// gracefully.
		Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (final Throwable e) {
			System.out.println(e.getMessage());
			System.exit(1);
		}
		System.exit(0);
	}

}