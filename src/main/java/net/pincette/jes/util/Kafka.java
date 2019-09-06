package net.pincette.jes.util;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Pair.pair;

import com.typesafe.config.Config;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Some Kafka utilities.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Kafka {
  private static final Map<String, Object> RELIABLE_PRODUCER_CONFIG =
      unmodifiableMap(
          map(
              pair("acks", "all"),
              pair("enable.idempotence", true),
              pair("request.timeout.ms", 5000),
              // Lower than replica.lag.time.max.ms, so few retries because that may generate
              // a lot of duplicates.
              pair("max.in.flight.requests.per.connection", 1)));

  private Kafka() {}

  /**
   * This creates a fail fast Kafka producer that demands full acknowledgement of sent messages.
   *
   * @param config the Kafka configuration, which will be merged with a reliable built-in
   *     configuration.
   * @param keySerializer the serializer for keys.
   * @param valueSerializer the serializer for values.
   * @param <K> the key type.
   * @param <V> the value type.
   * @return The Kafka producer.
   * @since 1.0
   */
  public static <K, V> KafkaProducer<K, V> createReliableProducer(
      final Map<String, Object> config,
      final Serializer<K> keySerializer,
      final Serializer<V> valueSerializer) {
    return new KafkaProducer<>(
        merge(config, RELIABLE_PRODUCER_CONFIG), keySerializer, valueSerializer);
  }

  /**
   * Gets the configuration object at <code>path</code> in <code>config</code> and flattens the tree
   * under it so that the keys in the resulting map are dot-separated paths as Kafka expects it.
   *
   * @param config the given configuration object.
   * @param path the dot-separated path withing the configuration object.
   * @return The Kafka configuration.
   * @since 1.0
   */
  public static Map<String, Object> fromConfig(final Config config, final String path) {
    return config.getConfig(path).entrySet().stream()
        .collect(toMap(Map.Entry::getKey, e -> e.getValue().unwrapped()));
  }

  /**
   * Sends a message to Kafka asynchronously.
   *
   * @param producer the used producer.
   * @param record the record to be sent.
   * @param <K> the key type.
   * @param <V> the value type.
   * @return <code>true</code> if the request was successful, <code>false</code> otherwise.
   * @since 1.0
   */
  public static <K, V> CompletionStage<Boolean> send(
      final KafkaProducer<K, V> producer, final ProducerRecord<K, V> record) {
    final CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();

    producer.send(
        record,
        (metadata, exception) -> {
          if (exception != null) {
            completableFuture.completeExceptionally(exception);
          } else {
            completableFuture.complete(true);
          }
        });

    return completableFuture;
  }
}
