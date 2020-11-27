package net.pincette.jes.util;

import static java.lang.System.getenv;
import static java.util.Arrays.stream;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static net.pincette.jes.util.JsonFields.CORR;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Collections.union;
import static net.pincette.util.Pair.pair;
import static org.apache.kafka.clients.producer.ProducerConfig.configNames;
import static org.apache.kafka.streams.kstream.JoinWindows.of;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;
import javax.json.JsonObject;
import net.pincette.util.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Some Kafka utilities.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Kafka {
  private static final String KAFKA_PREFIX = "KAFKA_";
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
   * Joins two streams on the <code>_corr</code> field. The result is keyed on that field, with the
   * values of both streams paired.
   *
   * @param stream1 the first stream.
   * @param stream2 the second stream.
   * @param window the join window.
   * @return The joined stream of pairs.
   * @since 1.1.4
   */
  public static KStream<String, Pair<JsonObject, JsonObject>> correlate(
      final KStream<String, JsonObject> stream1,
      final KStream<String, JsonObject> stream2,
      final Duration window) {
    return toCorr(stream1).join(toCorr(stream2), Pair::pair, of(window));
  }

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
        producerConfig(merge(config, RELIABLE_PRODUCER_CONFIG)), keySerializer, valueSerializer);
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
   * Reads environment variables that start with "KAFKA_" and creates a configuration with it. The
   * underscores become dots and the keywords are set in lower case.
   *
   * @return The Kafka configuration.
   * @since 1.3.3
   */
  public static Map<String, Object> fromEnv() {
    return kafkaEnv().collect(toMap(e -> kafkaProperty(e.getKey()), Entry::getValue));
  }

  private static Stream<Entry<String, String>> kafkaEnv() {
    return getenv().entrySet().stream().filter(e -> e.getKey().startsWith(KAFKA_PREFIX));
  }

  private static String kafkaProperty(final String env) {
    return stream(env.substring(KAFKA_PREFIX.length()).split("_"))
        .map(String::toLowerCase)
        .collect(joining("."));
  }

  private static Map<String, Object> producerConfig(final Map<String, Object> config) {
    final Set<String> names =
        union(
            configNames(),
            set("sasl.jaas.config", "sasl.mechanism", "ssl.endpoint.identification.algorithm"));

    return config.entrySet().stream()
        .filter(e -> names.contains(e.getKey()))
        .collect(toMap(Entry::getKey, Entry::getValue));
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

  private static KStream<String, JsonObject> toCorr(final KStream<String, JsonObject> stream) {
    return stream
        .filter((k, v) -> v.containsKey(CORR))
        .map((k, v) -> new KeyValue<>(v.getString(CORR).toLowerCase(), v));
  }
}
