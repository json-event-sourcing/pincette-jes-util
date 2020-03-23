package net.pincette.jes.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Base64.getEncoder;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.SEVERE;
import static java.util.stream.Collectors.toSet;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import static net.pincette.jes.util.JsonFields.SUB;
import static net.pincette.jes.util.JsonFields.SUBSCRIPTIONS;
import static net.pincette.jes.util.Util.getUsername;
import static net.pincette.json.JsonUtil.getObjects;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.util.Collections.union;
import static net.pincette.util.Util.tryToGet;
import static net.pincette.util.Util.tryToGetRethrow;
import static org.asynchttpclient.Dsl.asyncHttpClient;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.logging.Logger;
import javax.json.JsonObject;
import net.pincette.function.SideEffect;
import net.pincette.util.Collections;
import org.apache.kafka.streams.kstream.KStream;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.RequestBuilder;

/**
 * Utilities for fanout.io.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Fanout {
  private static final AsyncHttpClient client = asyncHttpClient();

  private Fanout() {}

  /**
   * Sets up Kafka Streams to connect the stream of replies of an aggregate to fanout channels base
   * on the username in the replies.
   *
   * @param replies the replies Kafka stream.
   * @param realmId the realm ID of the fanout account.
   * @param realmKey the realm key of the fanout account.
   * @since 1.0
   */
  public static void connect(
      final KStream<String, JsonObject> replies, final String realmId, final String realmKey) {
    replies.mapValues(
        v ->
            tryToGetRethrow(() -> send(v, realmId, realmKey).toCompletableFuture().get())
                .orElse(false));
  }

  /**
   * Sets up Kafka Streams to connect the stream of replies of an aggregate to fanout channels base
   * on the username in the replies.
   *
   * @param replies the replies Kafka stream.
   * @param realmId the realm ID of the fanout account.
   * @param realmKey the realm key of the fanout account.
   * @param logger the logger to which the exceptions will be sent. This means the consumer offset
   *     will be committed in any case.
   * @since 1.1.4
   */
  public static void connect(
      final KStream<String, JsonObject> replies,
      final String realmId,
      final String realmKey,
      final Logger logger) {
    replies.mapValues(
        v ->
            tryToGet(
                    () -> send(v, realmId, realmKey).toCompletableFuture().get(),
                    e ->
                        SideEffect.<Boolean>run(() -> logger.log(SEVERE, e.getMessage(), e))
                            .andThenGet(() -> false))
                .orElse(false));
  }

  private static JsonObject createMessage(final JsonObject json, final Set<String> usernames) {
    final String asString = string(json);

    return createObjectBuilder()
        .add(
            "items",
            usernames.stream()
                .reduce(
                    createArrayBuilder(),
                    (b, u) ->
                        b.add(
                            createObjectBuilder()
                                .add("channel", u)
                                .add(
                                    "formats",
                                    createObjectBuilder()
                                        .add(
                                            "http-stream",
                                            createObjectBuilder()
                                                .add(
                                                    "content",
                                                    "event: message\ndata:" + asString + "\n\n")))),
                    (b1, b2) -> b1))
        .build();
  }

  private static String password(final String realmId, final String realmKey) {
    return tryToGetRethrow(
            () -> getEncoder().encodeToString((realmId + ":" + realmKey).getBytes(UTF_8)))
        .orElse(null);
  }

  /**
   * Sends a message to a fanout channel if the username if present in the field <code>/_jwt/sub
   * </code>.
   *
   * @param json the message.
   * @param realmId the realm ID of the fanout account.
   * @param realmKey the realm key of the fanout account.
   * @return Whether it has succeeded or not.
   * @since 1.0
   */
  public static CompletionStage<Boolean> send(
      final JsonObject json, final String realmId, final String realmKey) {
    return usernames(json)
        .map(
            usernames ->
                client
                    .executeRequest(
                        new RequestBuilder()
                            .setUrl(url(realmId))
                            .setMethod("POST")
                            .addHeader("Content-Type", "application/json")
                            .addHeader("Authorization", "Basic " + password(realmId, realmKey))
                            .setBody(string(createMessage(json, usernames)))
                            .build())
                    .toCompletableFuture()
                    .thenApply(response -> response.getStatusCode() == 200))
        .orElseGet(() -> completedFuture(true));
  }

  private static Set<String> subscribers(final JsonObject json) {
    return getObjects(json, SUBSCRIPTIONS)
        .map(subscription -> subscription.getString(SUB, null))
        .collect(toSet());
  }

  private static String url(final String realmId) {
    return "https://api.fanout.io/realm/" + realmId + "/publish/";
  }

  private static Optional<Set<String>> usernames(final JsonObject json) {
    return Optional.of(
            union(
                getUsername(json).map(Collections::set).orElseGet(HashSet::new), subscribers(json)))
        .filter(usernames -> !usernames.isEmpty());
  }
}
