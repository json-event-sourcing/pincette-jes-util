package net.pincette.jes.util;

import static java.lang.Runtime.getRuntime;
import static java.time.Duration.ofSeconds;
import static java.util.stream.Collectors.toList;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.tryToDoRethrow;
import static org.apache.kafka.streams.KafkaStreams.State.ERROR;
import static org.apache.kafka.streams.StreamsConfig.AT_LEAST_ONCE;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import net.pincette.function.SideEffect;
import net.pincette.util.Pair;
import net.pincette.util.TimedCache;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;

/**
 * A few Kafka Streams utilities.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Streams {
  private Streams() {}

  private static void closeStreams(final List<KafkaStreams> streams, final CountDownLatch latch) {
    streams.forEach(
        s -> {
          s.close(ofSeconds(5));
          s.cleanUp();
          latch.countDown();
        });
  }

  /**
   * Filters the <code>stream</code> leaving out duplicates according to the <code>criterion</code>
   * over the time <code>window</code>. The stream will be marked for re-partitioning, but the keys
   * won't change.
   *
   * @param stream the given stream.
   * @param criterion the given criterion.
   * @param window the time window to look at.
   * @param <K> the key type.
   * @param <V> the value type.
   * @param <U> the criterion type.
   * @return The filtered stream.
   * @since 1.0.2
   */
  public static <K, V, U> KStream<K, V> duplicateFilter(
      final KStream<K, V> stream, final BiFunction<K, V, U> criterion, final Duration window) {
    final TimedCache<U, U> cache = new TimedCache<>(window);

    return stream
        .filterNot((k, v) -> cache.get(criterion.apply(k, v)).isPresent())
        .map(
            (k, v) ->
                Optional.of(criterion.apply(k, v))
                    .map(
                        c ->
                            SideEffect.<KeyValue<K, V>>run(() -> cache.put(c, c))
                                .andThenGet(() -> new KeyValue<>(k, v)))
                    .orElse(null));
  }

  private static Properties copy(final Properties properties) {
    final Properties result = new Properties();

    properties
        .stringPropertyNames()
        .forEach(name -> result.setProperty(name, properties.getProperty(name)));

    return result;
  }

  /**
   * Starting from <code>path</code> all underlying properties are collected in a properties object.
   * If there is structure in the underlying properties then they are flattened, resulting in
   * dot-separated keys.
   *
   * @param config the configuration object to read from.
   * @param path the path in the configuration from which to start reading.
   * @return The collected properties object.
   * @since 1.0
   */
  public static Properties fromConfig(final Config config, final String path) {
    return config.getConfig(path).entrySet().stream()
        .reduce(
            new Properties(),
            (p, e) -> {
              p.put(e.getKey(), e.getValue().unwrapped().toString());
              return p;
            },
            (p1, p2) -> p1);
  }

  /**
   * This starts the <code>topology</code> and waits for its completion. When the JVM is interrupted
   * the streams are closed.
   *
   * @param topology the topology to run.
   * @param properties the Kafka properties to run it with. The keys should be strings and the
   *     values JSON strings. In the KStreams and KTables the type for the values is always <code>
   *     javax.json.JsonObject</code>The processing guarantee will be set to at least once.
   * @return Indicates success of failure.
   * @see javax.json.JsonObject
   * @since 1.0
   */
  public static boolean start(final Topology topology, final Properties properties) {
    return start(Stream.of(pair(topology, properties)));
  }

  /**
   * This starts the <code>topologies</code> and waits for their completion. When the JVM is
   * interrupted or one of the streams fails all streams are closed.
   *
   * @param topologies the topologies to run with the Kafka properties to run them with. The keys
   *     should be strings and the values JSON strings. In the KStreams and KTables the type for the
   *     values is always <code>
   *     javax.json.JsonObject</code>The processing guarantee will be set to at least once.
   * @return Indicates success of failure of any of the topologies.
   * @see javax.json.JsonObject
   * @since 1.3.1
   */
  public static boolean start(final Stream<Pair<Topology, Properties>> topologies) {
    final List<Pair<Topology, Properties>> tpls = topologies.collect(toList());
    final boolean[] error = new boolean[1];
    final CountDownLatch latch = new CountDownLatch(tpls.size());
    final List<KafkaStreams> streams =
        tpls.stream()
            .map(t -> new KafkaStreams(t.first, streamsConfig(t.second)))
            .collect(toList());

    streams.forEach(
        s -> {
          s.setStateListener(
              (newState, oldState) -> {
                if (newState.equals(ERROR)) {
                  error[0] = true;
                  closeStreams(streams, latch);
                }
              });
          s.start();
        });

    getRuntime().addShutdownHook(new Thread(() -> closeStreams(streams, latch)));
    tryToDoRethrow(latch::await);

    return !error[0];
  }

  /**
   * Creates a new properties object with the default values for the streams configuration. It sets
   * the JSON serializer. The processing guarantee is set to "at least once". This is done only when
   * those properties are not yet present in <code>kafkaConfig</code>.
   *
   * @param kafkaConfig the given Kafka configuration.
   * @return The new configuration with the default values.
   * @see JsonSerde
   * @since 1.1
   */
  public static Properties streamsConfig(final Properties kafkaConfig) {
    final Properties result = copy(kafkaConfig);

    if (result.getProperty(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG) == null) {
      result.put(
          DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
          LogAndContinueExceptionHandler.class);
    }

    if (result.getProperty(DEFAULT_KEY_SERDE_CLASS_CONFIG) == null) {
      result.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    if (result.getProperty(DEFAULT_VALUE_SERDE_CLASS_CONFIG) == null) {
      result.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
    }

    if (result.getProperty(PROCESSING_GUARANTEE_CONFIG) == null) {
      result.put(PROCESSING_GUARANTEE_CONFIG, AT_LEAST_ONCE);
    }

    return result;
  }
}
