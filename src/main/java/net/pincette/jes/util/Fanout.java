package net.pincette.jes.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Base64.getEncoder;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import static net.pincette.jes.util.Util.getUsername;
import static net.pincette.util.Json.string;
import static net.pincette.util.Util.tryToGetRethrow;
import static org.asynchttpclient.Dsl.asyncHttpClient;

import java.util.concurrent.CompletionStage;
import javax.json.JsonObject;
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

  private static JsonObject createMessage(final JsonObject json, final String username) {
    return createObjectBuilder()
        .add(
            "items",
            createArrayBuilder()
                .add(
                    createObjectBuilder()
                        .add("channel", username)
                        .add(
                            "formats",
                            createObjectBuilder()
                                .add(
                                    "http-stream",
                                    createObjectBuilder()
                                        .add(
                                            "content",
                                            "event: message\ndata:" + string(json) + "\n\n")))))
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
    return getUsername(json)
        .map(
            username ->
                client
                    .executeRequest(
                        new RequestBuilder()
                            .setUrl(url(realmId))
                            .setMethod("POST")
                            .addHeader("Content-Type", "application/json")
                            .addHeader("Authorization", "Basic " + password(realmId, realmKey))
                            .setBody(string(createMessage(json, username)))
                            .build())
                    .toCompletableFuture()
                    .thenApply(response -> response.getStatusCode() == 200))
        .orElseGet(() -> completedFuture(true));
  }

  private static String url(final String realmId) {
    return "https://api.fanout.io/realm/" + realmId + "/publish/";
  }
}
