package net.pincette.jes.util;

import static java.lang.Runtime.getRuntime;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.tryToDoRethrow;
import static org.apache.kafka.streams.KafkaStreams.State.ERROR;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.AT_LEAST_ONCE;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import net.pincette.function.SideEffect;
import net.pincette.util.Collections;
import net.pincette.util.Pair;
import net.pincette.util.TimedCache;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription.Node;
import org.apache.kafka.streams.TopologyDescription.Sink;
import org.apache.kafka.streams.TopologyDescription.Source;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;

/**
 * A few Kafka Streams utilities.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Streams {
  private static final String ACTION = "action";
  private static final String APPLICATION = "application";
  private static final String IN = "in";
  private static final String OUT = "out";
  private static final String START = "start";
  private static final String STOP = "stop";

  private Streams() {}

  private static void closeStreams(
      final List<TopologyEntry> topologies,
      final CountDownLatch latch,
      final TopologyLifeCycle lifeCycle) {
    if (lifeCycle != null) {
      topologies.forEach(t -> lifeCycle.stopped(t.topology, getApplication(t.properties)));
    }

    topologies.forEach(
        t -> {
          closeStreams(t.streams);
          latch.countDown();
        });
  }

  private static void closeStreams(final KafkaStreams streams) {
    streams.close();
    streams.cleanUp();
  }

  private static Properties copy(final Properties properties) {
    final Properties result = new Properties();

    properties
        .stringPropertyNames()
        .forEach(name -> result.setProperty(name, properties.getProperty(name)));

    return result;
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

  private static String getApplication(final Properties properties) {
    return properties.getProperty(APPLICATION_ID_CONFIG);
  }

  private static Stream<Node> getNodes(final Topology topology) {
    return getNodes(
        topology.describe().subtopologies().stream().flatMap(s -> s.nodes().stream()),
        new HashSet<>());
  }

  private static Stream<Node> getNodes(final Stream<Node> nodes, final Set<Node> seen) {
    return nodes
        .filter(n -> !seen.contains(n))
        .map(n -> SideEffect.<Node>run(() -> seen.add(n)).andThenGet(() -> n))
        .flatMap(
            n ->
                concat(
                    Stream.of(n),
                    getNodes(concat(n.predecessors().stream(), n.successors().stream()), seen)));
  }

  private static boolean internalTopic(final String topic) {
    return topic.contains("KSTREAM");
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
    return start(topology, properties, null);
  }

  /**
   * This starts the <code>topology</code> and waits for its completion. When the JVM is interrupted
   * the streams are closed.
   *
   * @param topology the topology to run.
   * @param properties the Kafka properties to run it with. In the KStreams and KTables the type for
   *     the values is always <code>
   *     javax.json.JsonObject</code>The processing guarantee will be set to at least once.
   * @param lifeCycle the object that is called when a topology is started and stopped.
   * @return Indicates success of failure of any of the topologies.
   * @return Indicates success of failure.
   * @see javax.json.JsonObject
   * @since 1.3.6
   */
  public static boolean start(
      final Topology topology, final Properties properties, final TopologyLifeCycle lifeCycle) {
    return start(Stream.of(pair(topology, properties)), lifeCycle);
  }

  /**
   * This starts the <code>topologies</code> and waits for their completion. When the JVM is
   * interrupted or one of the streams fails all streams are closed.
   *
   * @param topologies the topologies to run with the Kafka properties to run them with. In the
   *     KStreams and KTables the type for the values is always <code>
   *     javax.json.JsonObject</code>The processing guarantee will be set to at least once.
   * @return Indicates success of failure of any of the topologies.
   * @see javax.json.JsonObject
   * @since 1.3.1
   */
  public static boolean start(final Stream<Pair<Topology, Properties>> topologies) {
    return start(topologies, null);
  }

  /**
   * This starts the <code>topologies</code> and waits for their completion. When the JVM is
   * interrupted or one of the streams fails all streams are closed.
   *
   * @param topologies the topologies to run with the Kafka properties to run them with. The keys
   *     should be strings and the values JSON strings. In the KStreams and KTables the type for the
   *     values is always <code>
   *     javax.json.JsonObject</code>The processing guarantee will be set to at least once.
   * @param lifeCycle the object that is called when a topology is started and stopped.
   * @return Indicates success of failure of any of the topologies.
   * @see javax.json.JsonObject
   * @since 1.3.6
   */
  public static boolean start(
      final Stream<Pair<Topology, Properties>> topologies, final TopologyLifeCycle lifeCycle) {
    final List<TopologyEntry> tpls = topologyEntries(topologies);
    final boolean[] error = new boolean[1];
    final CountDownLatch latch = new CountDownLatch(tpls.size());

    tpls.forEach(
        t -> {
          t.streams.setStateListener(
              (newState, oldState) -> {
                if (newState.equals(ERROR)) {
                  error[0] = true;
                  closeStreams(tpls, latch, lifeCycle);
                }
              });
          t.streams.start();

          if (lifeCycle != null) {
            lifeCycle.started(t.topology, getApplication(t.properties));
          }
        });

    getRuntime().addShutdownHook(new Thread(() -> closeStreams(tpls, latch, lifeCycle)));
    tryToDoRethrow(latch::await);

    return !error[0];
  }

  /**
   * Starts a Kafka Streams topology without blocking.
   *
   * @param topology the topology to run.
   * @param properties the Kafka properties to run it with. In the KStreams and KTables the type for
   *     the values is always <code>
   *     javax.json.JsonObject</code>The processing guarantee will be set to at least once.
   * @param lifeCycle the object that is called when a topology is started and stopped.
   * @param onError the function that is called when a topology goes into the error state. Normally
   *     you have to stop the topology completely, because it won't work anymore. Therefore, the
   *     first argument is the stop function. The second one is the name of the application.
   * @return The function with which the topology can be stopped.
   * @since 1.3.8
   */
  public static Stop start(
      final Topology topology,
      final Properties properties,
      final TopologyLifeCycle lifeCycle,
      final BiConsumer<Stop, String> onError) {
    return start(topology, properties, lifeCycle, onError, null);
  }

  /**
   * Starts a Kafka Streams topology without blocking.
   *
   * @param topology the topology to run.
   * @param properties the Kafka properties to run it with. In the KStreams and KTables the type for
   *     the values is always <code>
   *     javax.json.JsonObject</code>The processing guarantee will be set to at least once.
   * @param lifeCycle the object that is called when a topology is started and stopped.
   * @param onError the function that is called when a topology goes into the error state. Normally
   *     you have to stop the topology completely, because it won't work anymore. Therefore, the
   *     first argument is the stop function. The second one is the name of the application.
   * @param uncaughtExceptions the handler for uncaught exceptions.
   * @return The function with which the topology can be stopped.
   * @since 1.3.12
   */
  public static Stop start(
      final Topology topology,
      final Properties properties,
      final TopologyLifeCycle lifeCycle,
      final BiConsumer<Stop, String> onError,
      final Consumer<Throwable> uncaughtExceptions) {
    final String application = getApplication(properties);
    final KafkaStreams streams = new KafkaStreams(topology, streamsConfig(properties));
    final Stop stop = new Stopper(streams, topology, lifeCycle, application);

    streams.setStateListener(
        (newState, oldState) -> {
          if (newState.equals(ERROR)) {
            onError.accept(stop, application);
          }
        });

    if (uncaughtExceptions != null) {
      streams.setUncaughtExceptionHandler(
          e -> {
            uncaughtExceptions.accept(e);
            return REPLACE_THREAD;
          });
    }

    streams.start();

    if (lifeCycle != null) {
      lifeCycle.started(topology, application);
    }

    return stop;
  }

  public static JsonObject startMessage(
      final String application, final Set<String> inputTopics, final Set<String> outputTopics) {
    return startStopMessage(application, inputTopics, outputTopics, START);
  }

  private static JsonObject startStopMessage(
      final String application,
      final Set<String> inputTopics,
      final Set<String> outputTopics,
      final String action) {
    return createObjectBuilder()
        .add(APPLICATION, application)
        .add(IN, toJsonArray(inputTopics))
        .add(OUT, toJsonArray(outputTopics))
        .add(ACTION, action)
        .build();
  }

  public static JsonObject stopMessage(
      final String application, final Set<String> inputTopics, final Set<String> outputTopics) {
    return startStopMessage(application, inputTopics, outputTopics, STOP);
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

  private static JsonArrayBuilder toJsonArray(final Set<String> values) {
    return values.stream().reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1);
  }

  /**
   * Returns a pair of source and sink topics the given topology uses. This only includes the
   * external topics, not the ones that are generated automatically by Kafka Streams. This way you
   * get the external interface of the topology.
   *
   * @param topology the given topology.
   * @return The collected pair.
   * @since 1.3.6
   */
  public static Pair<Set<String>, Set<String>> topics(final Topology topology) {
    return getNodes(topology)
        .filter(n -> n instanceof Source || n instanceof Sink)
        .reduce(
            pair(new HashSet<>(), new HashSet<>()),
            (p, n) -> {
              if (n instanceof Source) {
                p.first.addAll(topics((Source) n));
              } else {
                p.second.addAll(topics((Sink) n));
              }

              return p;
            },
            (p1, p2) -> p1);
  }

  private static Set<String> topics(final Source source) {
    return source.topicSet().stream().filter(t -> !internalTopic(t)).collect(toSet());
  }

  private static Set<String> topics(final Sink sink) {
    return ofNullable(sink.topic())
        .filter(t -> !internalTopic(t))
        .map(Collections::set)
        .orElseGet(java.util.Collections::emptySet);
  }

  private static List<TopologyEntry> topologyEntries(
      final Stream<Pair<Topology, Properties>> topologies) {
    return topologies
        .map(
            t ->
                new TopologyEntry(
                    t.first, t.second, new KafkaStreams(t.first, streamsConfig(t.second))))
        .collect(toList());
  }

  @FunctionalInterface
  public interface Stop {
    void stop();
  }

  public interface TopologyLifeCycle {
    void started(Topology topology, String application);

    void stopped(Topology topology, String application);
  }

  private static class Stopper implements Stop {
    private final String application;
    private final TopologyLifeCycle lifeCycle;
    private final KafkaStreams streams;
    private final Topology topology;
    private boolean stopped;

    private Stopper(
        final KafkaStreams streams,
        final Topology topology,
        final TopologyLifeCycle lifeCycle,
        final String application) {
      this.streams = streams;
      this.topology = topology;
      this.lifeCycle = lifeCycle;
      this.application = application;
    }

    public void stop() {
      if (!stopped) {
        closeStreams(streams);

        if (lifeCycle != null) {
          lifeCycle.stopped(topology, application);
        }

        stopped = true;
      }
    }
  }

  private static class TopologyEntry {
    private final Properties properties;
    private final KafkaStreams streams;
    private final Topology topology;

    private TopologyEntry(
        final Topology topology, final Properties properties, final KafkaStreams streams) {
      this.topology = topology;
      this.properties = properties;
      this.streams = streams;
    }
  }

  public static class TopologyLifeCycleEmitter implements TopologyLifeCycle {
    private final KafkaProducer<String, JsonObject> producer;
    private final String topic;

    public TopologyLifeCycleEmitter(
        final String topic, final KafkaProducer<String, JsonObject> producer) {
      this.topic = topic;
      this.producer = producer;
    }

    private void sendMessage(
        final Topology topology, final String application, final String action) {
      final Pair<Set<String>, Set<String>> topics = topics(topology);

      send(
              producer,
              new ProducerRecord<>(
                  topic,
                  randomUUID().toString(),
                  startStopMessage(application, topics.first, topics.second, action)))
          .toCompletableFuture()
          .join();
    }

    public void started(final Topology topology, final String application) {
      sendMessage(topology, application, START);
    }

    public void stopped(final Topology topology, final String application) {
      sendMessage(topology, application, STOP);
    }
  }
}
