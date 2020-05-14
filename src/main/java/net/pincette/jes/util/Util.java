package net.pincette.jes.util;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.jes.util.Command.hasError;
import static net.pincette.jes.util.Event.applyEvent;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.SEQ;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.json.JsonUtil.getNumber;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.filter.Util.stream;

import java.util.Optional;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.stream.JsonParser;
import net.pincette.json.JsonUtil;

/**
 * Some general utilities.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Util {
  private Util() {}

  /**
   * Returns a reducer that first calls <code>validator</code> and if the result doesn't contain any
   * errors it calls <code>reducer</code>.
   *
   * @param validator the given validator.
   * @param reducer the given reducer.
   * @return The composed function.
   * @since 1.0
   */
  public static Reducer compose(final Reducer validator, final Reducer reducer) {
    return (command, aggregate) ->
        validator
            .apply(command, aggregate)
            .thenComposeAsync(
                result ->
                    hasError(result) ? completedFuture(result) : reducer.apply(command, aggregate));
  }

  /**
   * Returns the username from "/_jwt/sub".
   *
   * @param json the given object.
   * @return The username.
   * @since 1.0
   */
  public static Optional<String> getUsername(final JsonObject json) {
    return getString(json, "/_jwt/sub");
  }

  /**
   * Checks if <code>json</code> is a JSON Event Sourcing object. This means it has the fields
   * <code>_id</code> and <code>_type</code>.
   *
   * @param json the given object.
   * @return The check report.
   * @see JsonFields
   * @since 1.1.2
   */
  public static boolean isJesObject(final JsonObject json) {
    return json != null
        && getString(json, "/" + ID).isPresent()
        && getString(json, "/" + TYPE).isPresent();
  }

  /**
   * Checks if <code>json</code> is a managed object. This means it has the fields <code>_id</code>,
   * <code>_type</code> and <code>_seq</code>.
   *
   * @param json the given object.
   * @return The check report.
   * @see JsonFields
   * @since 1.0
   */
  public static boolean isManagedObject(final JsonObject json) {
    return isJesObject(json) && getNumber(json, "/" + SEQ).isPresent();
  }

  /**
   * Checks if <code>json</code> is a managed object with the fields <code>_id</code> and <code>
   * _type</code> set to <code>id</code> and <code>type</code> respectively.
   *
   * @param json the given object.
   * @param type the aggregate type.
   * @param id the ID of the instance.
   * @return The check report.
   * @since 1.0
   */
  public static boolean isManagedObject(final JsonObject json, final String type, final String id) {
    return isManagedObject(json)
        && type != null
        && id != null
        && getString(json, "/" + TYPE).filter(t -> t.equals(type)).isPresent()
        && getString(json, "/" + ID).filter(i -> i.equalsIgnoreCase(id)).isPresent();
  }

  /**
   * Reconstructs aggregate instances using a sequence of events.
   *
   * @param events the array of events, which should start at <code>_seq</code> equal to 0 and which
   *     should no holes in the numbering.
   * @return The reconstructed aggregate instance stream. The events are applied one after the other
   *     and each intermediate aggregate instance is emitted in the stream.
   * @since 1.2
   */
  public static Stream<JsonObject> reconstruct(final JsonArray events) {
    return reconstruct(events.stream());
  }

  /**
   * Reconstructs aggregate instances using a sequence of events.
   *
   * @param events the event parser, which should start returning objects at <code>_seq</code> equal
   *     to 0 and which should no holes in the numbering.
   * @return The reconstructed aggregate instance stream. The events are applied one after the other
   *     and each intermediate aggregate instance is emitted in the stream.
   * @since 1.2
   */
  public static Stream<JsonObject> reconstruct(final JsonParser events) {
    return reconstruct(stream(events));
  }

  /**
   * Reconstructs aggregate instances using a sequence of events.
   *
   * @param events the stream of events, which should start at <code>_seq</code> equal to 0 and
   *     which should no holes in the numbering.
   * @return The reconstructed aggregate instance stream. The events are applied one after the other
   *     and each intermediate aggregate instance is emitted in the stream.
   * @since 1.2
   */
  public static Stream<JsonObject> reconstruct(final Stream<? extends JsonValue> events) {
    return events.filter(JsonUtil::isObject).map(JsonValue::asJsonObject).map(applyEvent());
  }
}
