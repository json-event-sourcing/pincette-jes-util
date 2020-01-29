package net.pincette.jes.util;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.SEQ;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.Validate.hasErrors;

import java.util.Optional;
import javax.json.JsonObject;

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
    return (aggregate, command) ->
        validator
            .apply(aggregate, command)
            .thenComposeAsync(
                result ->
                    hasErrors(result)
                        ? completedFuture(result)
                        : reducer.apply(aggregate, command));
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
    return json != null && json.containsKey(ID) && json.containsKey(TYPE);
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
    return isJesObject(json) && json.containsKey(SEQ);
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
        && type.equals(json.getString(TYPE))
        && id.equalsIgnoreCase(json.getString(ID));
  }
}
