package net.pincette.jes.util;

import static net.pincette.jes.util.JsonFields.BEFORE;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.OPS;
import static net.pincette.jes.util.JsonFields.SEQ;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.jes.util.Util.isManagedObject;
import static net.pincette.util.Json.getValue;
import static net.pincette.util.Pair.pair;

import java.util.Optional;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.util.Json;

/**
 * Utilities to work with events.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Event {
  private Event() {}

  /**
   * Returns <code>true</code> if the field at <code>jsonPointer</code> has changed. It examines the
   * <code>_ops</code> field for this.
   *
   * @param event the event.
   * @param jsonPointer the path into the aggregate.
   * @return <code>true</code> when the event expresses a change of the given field.
   * @since 1.0
   */
  public static boolean changed(final JsonObject event, final String jsonPointer) {
    return Optional.ofNullable(event.getJsonArray(OPS))
        .map(ops -> Json.changed(ops, jsonPointer))
        .orElse(false);
  }

  /**
   * Returns <code>true</code> if the field at <code>jsonPointer</code> has changed from the value
   * in <code>from</code> to the value in <code>to</code>. It examines the <code>_ops</code> field
   * for this.
   *
   * @param event the event.
   * @param jsonPointer the path into the aggregate.
   * @param from the original value.
   * @param to the new value.
   * @return <code>true</code> when the event expresses a change of the given field.
   * @since 1.0
   */
  public static boolean changed(
      final JsonObject event, final String jsonPointer, final JsonValue from, final JsonValue to) {
    return Optional.ofNullable(event.getJsonArray(OPS))
        .flatMap(
            ops ->
                Optional.ofNullable(event.getJsonObject(BEFORE))
                    .filter(
                        before ->
                            getValue(before, jsonPointer)
                                .filter(value -> value.equals(from))
                                .isPresent())
                    .map(before -> pair(ops, before)))
        .map(pair -> Json.changed(pair.first, pair.second, jsonPointer, from, to))
        .orElse(false);
  }

  private static boolean hasOps(final JsonObject event) {
    return Optional.ofNullable(event.getJsonArray("_ops"))
        .filter(ops -> !ops.isEmpty())
        .isPresent();
  }

  /**
   * Checks if <code>event</code> has the proper event structure. This means it should be a managed
   * object with an <code>_ops</code> field.
   *
   * @param event the given event.
   * @return The check report.
   * @see JsonFields
   * @see Util#isManagedObject
   * @since 1.0
   */
  public static boolean isEvent(final JsonObject event) {
    return isManagedObject(event) && hasOps(event);
  }

  /**
   * Checks if the event has the next sequence number for the aggregate. Event log replicators can
   * use this to detect corruption.
   *
   * @param aggregate the aggregate instance.
   * @param event the event.
   * @return <code>true</code> if the event is the next one, <code>false</code> otherwise.
   * @since 1.0
   */
  public static boolean isNext(final JsonObject aggregate, final JsonObject event) {
    return event.getInt(SEQ) == aggregate.getInt(SEQ) + 1;
  }

  /**
   * Creates a standard message to alert event sequence errors. Events are numbered with the <code>
   * _seq</code> field. When an event listener detect a "hole" in the numbering it can use this
   * message to log the corruption.
   *
   * @param aggregate the aggregate about which the error message is produced.
   * @param event the event about which the error message is produced.
   * @return The generated error message.
   * @since 1.0
   */
  public static String sequenceErrorMessage(final JsonObject aggregate, final JsonObject event) {
    return "SEQUENCE ERROR: event received for ("
        + aggregate.getString(ID)
        + ","
        + aggregate.getString(TYPE)
        + ") with sequence number "
        + event.getInt(SEQ)
        + ", while "
        + (aggregate.getInt(SEQ) + 1)
        + " was expected";
  }
}
