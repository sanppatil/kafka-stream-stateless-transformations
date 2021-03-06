package com.kafka.stream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class BranchStreams {

	public static void main(String[] args) {
		// Set up the configuration.
		final Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "branch-stream");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		// Get the source stream.
		final StreamsBuilder builder = new StreamsBuilder();
		final KStream<String, String> source = builder.stream("streams-branch-topic");

		@SuppressWarnings("unchecked")
		KStream<String, String>[] branches = source.branch((key, value) -> (key.length() % 2) == 0,
				(key, value) -> true);

		KStream<String, String> evenKeysStream = branches[0];
		KStream<String, String> oddKeysStream = branches[1];

		evenKeysStream.to("streams-even-key-topic");
		oddKeysStream.to("streams-odd-key-topic");

		final Topology topology = builder.build();
		final KafkaStreams streams = new KafkaStreams(topology, props);
		// Print the topology to the console.
		System.out.println(topology.describe());
		final CountDownLatch latch = new CountDownLatch(1);

		// Attach a shutdown handler to catch control-c and terminate gracefully.
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