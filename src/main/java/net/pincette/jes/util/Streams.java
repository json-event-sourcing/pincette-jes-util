package net.pincette.jes.util;

import static java.lang.Runtime.getRuntime;
import static net.pincette.util.Util.tryToGetWithRethrow;
import static org.apache.kafka.streams.KafkaStreams.State.ERROR;
import static org.apache.kafka.streams.StreamsConfig.AT_LEAST_ONCE;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import net.pincette.function.SideEffect;
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
              p.put(e.getKey(), e.getValue().unwrapped());
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
   *     javax.json.JsonObject</code>The processing guarantee will be set to exactly once.
   * @return Indicates success of failure.
   * @see <a
   *     href="https://static.javadoc.io/javax.json/javax.json-api/1.1.4/javax/json/JsonObject.html">JsonObject</a>
   * @since 1.0
   */
  public static boolean start(final Topology topology, final Properties properties) {
    final boolean[] error = new boolean[1];
    final CountDownLatch latch = new CountDownLatch(1);

    properties.put(
        DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueExceptionHandler.class);
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
    properties.put(PROCESSING_GUARANTEE_CONFIG, AT_LEAST_ONCE);

    return tryToGetWithRethrow(
            () -> new KafkaStreams(topology, properties),
            streams -> {
              streams.setStateListener(
                  (newState, oldState) -> {
                    if (newState.equals(ERROR)) {
                      error[0] = true;
                      latch.countDown();
                    }
                  });

              getRuntime()
                  .addShutdownHook(
                      new Thread(
                          () -> {
                            streams.close();
                            latch.countDown();
                          }));

              streams.start();
              latch.await();

              return !error[0];
            })
        .orElse(false);
  }
}
