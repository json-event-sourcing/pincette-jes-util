package net.pincette.jes.util;

import static java.lang.String.valueOf;
import static java.lang.System.getenv;
import static java.util.Arrays.stream;
import static java.util.Collections.unmodifiableMap;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Collections.union;
import static net.pincette.util.Pair.pair;
import static org.apache.kafka.clients.admin.AdminClientConfig.configNames;

import com.typesafe.config.Config;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;
import javax.json.JsonObject;
import net.pincette.util.State;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.OffsetSpec.LatestSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Some Kafka utilities.
 *
 * @author Werner Donné
 * @since 1.0
 */
public class Kafka {
  private static final Set<String> KAFKA_ADMIN_CONFIG_NAMES =
      union(
          configNames(),
          set("ssl.endpoint.identification.algorithm", "sasl.mechanism", "sasl.jaas.config"));
  private static final String KAFKA_PREFIX = "KAFKA_";
  private static final Map<String, Object> RELIABLE_PRODUCER_CONFIG =
      unmodifiableMap(
          map(
              pair("acks", "all"),
              pair("enable.idempotence", true),
              pair("max.in.flight.requests.per.connection", 1)));

  private Kafka() {}

  /**
   * Returns a new config with only admin properties.
   *
   * @param config the given config.
   * @return The new config.
   * @since 3.1
   */
  public static Map<String, Object> adminConfig(final Map<String, Object> config) {
    return config.entrySet().stream()
        .filter(e -> KAFKA_ADMIN_CONFIG_NAMES.contains(e.getKey()))
        .collect(toMap(Entry::getKey, Entry::getValue));
  }

  private static CompletionStage<Map<String, Map<TopicPartition, Long>>> consumerGroupOffsets(
      final Collection<ConsumerGroupListing> groups,
      final Collection<TopicPartition> partitions,
      final Admin admin) {
    return consumerGroupOffsets(
        groups.stream().map(ConsumerGroupListing::groupId).collect(toSet()), partitions, admin);
  }

  /**
   * Returns the offsets for the given consumer groups for the given topic partitions.
   *
   * @param groups the given consumer groups.
   * @param partitions the given topic partitions.
   * @param admin the Kafka admin object.
   * @return The offsets per consumer group.
   * @since 3.2.0
   */
  public static CompletionStage<Map<String, Map<TopicPartition, Long>>> consumerGroupOffsets(
      final Set<String> groups, final Collection<TopicPartition> partitions, final Admin admin) {
    final ListConsumerGroupOffsetsSpec spec =
        new ListConsumerGroupOffsetsSpec().topicPartitions(partitions);

    return admin
        .listConsumerGroupOffsets(map(groups.stream().map(g -> pair(g, spec))))
        .all()
        .toCompletionStage()
        .thenApply(
            offsets ->
                offsets.entrySet().stream()
                    .collect(
                        toMap(
                            Entry::getKey, e -> toLong(e.getValue(), OffsetAndMetadata::offset))));
  }

  private static CompletionStage<Collection<ConsumerGroupListing>> consumerGroups(
      final Admin admin, final Predicate<String> includeGroup) {
    return admin
        .listConsumerGroups()
        .valid()
        .toCompletionStage()
        .thenApply(
            listing -> listing.stream().filter(l -> includeGroup.test(l.groupId())).toList());
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
   * Returns the consumer group information for the given groups.
   *
   * @param groups the given consumer groups.
   * @param admin the Kafka admin object.
   * @return The consumer group information per consumer group.
   * @since 3.2.0
   */
  public static CompletionStage<Map<String, ConsumerGroupDescription>> describeConsumerGroups(
      final Set<String> groups, final Admin admin) {
    return admin.describeConsumerGroups(groups).all().toCompletionStage();
  }

  /**
   * Flattens the tree under <code>config</code> so that the keys in the resulting map are
   * dot-separated paths as Kafka expects it.
   *
   * @param config the given configuration object.
   * @return The Kafka configuration.
   * @since 3.1
   */
  public static Map<String, Object> fromConfig(final Config config) {
    return config.entrySet().stream()
        .collect(toMap(Map.Entry::getKey, e -> e.getValue().unwrapped()));
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
    return fromConfig(config.getConfig(path));
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

  private static Map<TopicPartition, OffsetSpec> latest(
      final Collection<TopicPartition> partitions) {
    return partitions.stream().collect(toMap(p -> p, p -> new LatestSpec()));
  }

  /**
   * Returns all the message lags for all the non-internal topics.
   *
   * @param admin the Kafka admin object.
   * @return The completion stage with the map per consumer group.
   * @since 1.4
   */
  public static CompletionStage<Map<String, Map<TopicPartition, Long>>> messageLag(
      final Admin admin) {
    return messageLag(admin, g -> true);
  }

  /**
   * Returns all the message lags for the given topic.
   *
   * @param topic the given topic.
   * @param admin the Kafka admin object.
   * @return The completion stage with the map per consumer group.
   * @since 3.1
   */
  public static CompletionStage<Map<String, Map<TopicPartition, Long>>> messageLag(
      final String topic, final Admin admin) {
    return messageLag(topic, admin, g -> true);
  }

  /**
   * Returns all the message lags for all the non-internal topics.
   *
   * @param admin the Kafka admin object.
   * @param includeGroup the predicate that selects the consumer groups that should be included in
   *     the result.
   * @return The completion stage with the map per consumer group.
   * @since 2.0
   */
  public static CompletionStage<Map<String, Map<TopicPartition, Long>>> messageLag(
      final Admin admin, final Predicate<String> includeGroup) {
    return messageLag(
        () -> topicPartitions(admin), () -> topicPartitionOffsets(admin), admin, includeGroup);
  }

  /**
   * Returns all the message lags for the given topic.
   *
   * @param topic the given topic.
   * @param admin the Kafka admin object.
   * @param includeGroup the predicate that selects the consumer groups that should be included in
   *     the result.
   * @return The completion stage with the map per consumer group.
   * @since 3.1
   */
  public static CompletionStage<Map<String, Map<TopicPartition, Long>>> messageLag(
      final String topic, final Admin admin, final Predicate<String> includeGroup) {
    return messageLag(set(topic), admin, includeGroup);
  }

  /**
   * Returns all the message lags for the given topics.
   *
   * @param topics the given topics.
   * @param admin the Kafka admin object.
   * @param includeGroup the predicate that selects the consumer groups that should be included in
   *     the result.
   * @return The completion stage with the map per consumer group.
   * @since 3.1.7
   */
  public static CompletionStage<Map<String, Map<TopicPartition, Long>>> messageLag(
      final Set<String> topics, final Admin admin, final Predicate<String> includeGroup) {
    return messageLag(
        () -> topicPartitions(topics, admin),
        () -> topicPartitionOffsets(topics, admin),
        admin,
        includeGroup);
  }

  private static CompletionStage<Map<String, Map<TopicPartition, Long>>> messageLag(
      final Supplier<CompletionStage<Collection<TopicPartition>>> getPartitions,
      final Supplier<CompletionStage<Map<TopicPartition, Long>>> getPartitionOffsets,
      final Admin admin,
      final Predicate<String> includeGroup) {
    return consumerGroups(admin, includeGroup)
        .thenComposeAsync(
            groups ->
                getPartitions
                    .get()
                    .thenComposeAsync(
                        partitions -> consumerGroupOffsets(groups, partitions, admin)))
        .thenComposeAsync(
            groupOffsets ->
                getPartitionOffsets
                    .get()
                    .thenApply(
                        partitionOffsets -> messageLagPerGroup(groupOffsets, partitionOffsets)));
  }

  private static Map<TopicPartition, Long> messageLag(
      final Map<TopicPartition, Long> offsets, final Map<TopicPartition, Long> latest) {
    return offsets.entrySet().stream()
        .map(
            e ->
                ofNullable(latest.get(e.getKey()))
                    .map(v -> pair(e.getKey(), v - e.getValue()))
                    .orElse(null))
        .filter(Objects::nonNull)
        .collect(toMap(pair -> pair.first, pair -> pair.second));
  }

  private static Map<String, Map<TopicPartition, Long>> messageLagPerGroup(
      final Map<String, Map<TopicPartition, Long>> consumerGroupOffsets,
      final Map<TopicPartition, Long> partitionOffsets) {
    return consumerGroupOffsets.entrySet().stream()
        .map(e -> pair(e.getKey(), messageLag(e.getValue(), partitionOffsets)))
        .filter(pair -> !pair.second.isEmpty())
        .collect(toMap(pair -> pair.first, pair -> pair.second));
  }

  private static Set<String> nonInternal(final Collection<TopicListing> topics) {
    return topics.stream().filter(t -> !t.isInternal()).map(TopicListing::name).collect(toSet());
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
   * @param rec the record to be sent.
   * @param <K> the key type.
   * @param <V> the value type.
   * @return <code>true</code> if the request was successful, <code>false</code> otherwise.
   * @since 1.0
   */
  public static <K, V> CompletionStage<Boolean> send(
      final KafkaProducer<K, V> producer, final ProducerRecord<K, V> rec) {
    final CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();

    producer.send(
        rec,
        (metadata, exception) -> {
          if (exception != null) {
            completableFuture.completeExceptionally(exception);
          } else {
            completableFuture.complete(true);
          }
        });

    return completableFuture;
  }

  /**
   * Sends a list of messages to Kafka asynchronously.
   *
   * @param producer the used producer.
   * @param records the records to be sent.
   * @param <K> the key type.
   * @param <V> the value type.
   * @return <code>true</code> if the request was successful, <code>false</code> otherwise.
   * @since 3.1.10
   */
  public static <K, V> CompletionStage<Boolean> send(
      final KafkaProducer<K, V> producer, final List<ProducerRecord<K, V>> records) {
    final CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
    final State<Exception> state = new State<>();

    for (int i = 0; i < records.size() - 1; ++i) {
      producer.send(
          records.get(i),
          (metadata, exception) -> {
            if (exception != null) {
              state.set(exception);
            }
          });
    }

    producer.send(
        records.get(records.size() - 1),
        (metadata, exception) -> {
          if (exception != null) {
            completableFuture.completeExceptionally(exception);
          } else if (state.get() != null) {
            completableFuture.completeExceptionally(state.get());
          } else {
            completableFuture.complete(true);
          }
        });

    return completableFuture;
  }

  /**
   * Converts a message lag map to JSON.
   *
   * @param messageLags the given message lag map.
   * @return The JSON object.
   * @since 1.4
   */
  public static JsonObject toJson(final Map<String, Map<TopicPartition, Long>> messageLags) {
    return messageLags.entrySet().stream()
        .reduce(
            createObjectBuilder(),
            (b, e) -> b.add(e.getKey(), toJsonPerPartition(e.getValue())),
            (b1, b2) -> b1)
        .build();
  }

  private static JsonObject toJsonPerPartition(final Map<TopicPartition, Long> messageLags) {
    return from(
        messageLags.entrySet().stream()
            .collect(
                groupingBy(
                    e -> e.getKey().topic(),
                    toMap(e -> valueOf(e.getKey().partition()), Entry::getValue))));
  }

  private static <T> Map<TopicPartition, Long> toLong(
      final Map<TopicPartition, T> offsets, final ToLongFunction<T> fn) {
    return offsets.entrySet().stream()
        .filter(e -> e.getValue() != null)
        .collect(toMap(Entry::getKey, e -> fn.applyAsLong(e.getValue())));
  }

  /**
   * Converts topic descriptions to topic partitions.
   *
   * @param topics the given topic descriptions.
   * @return The topic partitions.
   * @since 3.1
   */
  public static Collection<TopicPartition> toPartitions(final Collection<TopicDescription> topics) {
    return topics.stream()
        .flatMap(t -> t.partitions().stream().map(p -> new TopicPartition(t.name(), p.partition())))
        .toList();
  }

  /**
   * Returns the latest offsets for each topic partition for all topics.
   *
   * @param admin the Kafka admin object.
   * @return The offsets per partition.
   * @since 3.2.0
   */
  public static CompletionStage<Map<TopicPartition, Long>> topicPartitionOffsets(
      final Admin admin) {
    return topicPartitionOffsets(() -> topicPartitions(admin), admin);
  }

  /**
   * Returns the latest offsets for each topic partition of the given topics.
   *
   * @param topics the given topics.
   * @param admin the Kafka admin object.
   * @return The offsets per partition.
   * @since 3.2.0
   */
  public static CompletionStage<Map<TopicPartition, Long>> topicPartitionOffsets(
      final Set<String> topics, final Admin admin) {
    return topicPartitionOffsets(() -> topicPartitions(topics, admin), admin);
  }

  private static CompletionStage<Map<TopicPartition, Long>> topicPartitionOffsets(
      final Supplier<CompletionStage<Collection<TopicPartition>>> getPartitions,
      final Admin admin) {
    return getPartitions
        .get()
        .thenComposeAsync(partitions -> topicPartitionOffsets(partitions, admin));
  }

  /**
   * Returns the latest offsets for the given topic partitions.
   *
   * @param partitions the given topic partitions.
   * @param admin the Kafka admin object.
   * @return The offsets per partition.
   * @since 3.2.0
   */
  public static CompletionStage<Map<TopicPartition, Long>> topicPartitionOffsets(
      final Collection<TopicPartition> partitions, final Admin admin) {
    return admin
        .listOffsets(latest(partitions))
        .all()
        .toCompletionStage()
        .thenApply(offsets -> toLong(offsets, ListOffsetsResultInfo::offset));
  }

  /**
   * Returns the topic partitions of a topic.
   *
   * @param topic the given topic.
   * @param admin the admin API.
   * @return The collection of topic partitions.
   * @since 3.1
   */
  public static CompletionStage<Collection<TopicPartition>> topicPartitions(
      final String topic, final Admin admin) {
    return topicPartitions(set(topic), admin);
  }

  /**
   * Returns all non-internal topics.
   *
   * @param admin the admin API.
   * @return The collection of topic partitions.
   * @since 3.1
   */
  public static CompletionStage<Collection<TopicPartition>> topicPartitions(final Admin admin) {
    return topics(admin)
        .thenApply(Kafka::nonInternal)
        .thenComposeAsync(names -> topicPartitions(names, admin));
  }

  /**
   * Returns the topic partitions of a set of topics.
   *
   * @param topics the given topics.
   * @param admin the admin API.
   * @return The collection of topic partitions.
   * @since 3.1
   */
  public static CompletionStage<Collection<TopicPartition>> topicPartitions(
      final Set<String> topics, final Admin admin) {
    return admin
        .describeTopics(topics)
        .allTopicNames()
        .toCompletionStage()
        .thenApply(Map::values)
        .thenApply(Kafka::toPartitions);
  }

  private static CompletionStage<Collection<TopicListing>> topics(final Admin admin) {
    return admin
        .listTopics(new ListTopicsOptions().listInternal(false))
        .listings()
        .toCompletionStage();
  }
}
