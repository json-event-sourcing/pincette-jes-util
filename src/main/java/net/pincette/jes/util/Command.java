package net.pincette.jes.util;

import static javax.json.Json.createObjectBuilder;
import static net.pincette.jes.util.JsonFields.COMMAND;
import static net.pincette.jes.util.JsonFields.CORR;
import static net.pincette.jes.util.JsonFields.ERROR;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.JWT;
import static net.pincette.jes.util.JsonFields.OPS;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.util.Builder.create;

import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

/**
 * Some command utilities.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Command {
  private Command() {}

  /**
   * Creates a command without data.
   *
   * @param type the aggregate type.
   * @param id the ID of the aggregate instance.
   * @param name the name of the command.
   * @return The JSON representation of the command.
   * @since 1.0
   */
  public static JsonObject createCommand(final String type, final String id, final String name) {
    return createCommandBuilder(type, id, name).build();
  }

  /**
   * Creates a command builder without data. You can then add more fields to it.
   *
   * @param type the aggregate type.
   * @param id the ID of the aggregate instance.
   * @param name the name of the command.
   * @return The JSON builder.
   * @since 1.0
   */
  public static JsonObjectBuilder createCommandBuilder(
      final String type, final String id, final String name) {
    return createObjectBuilder().add(TYPE, type).add(ID, id.toLowerCase()).add(COMMAND, name);
  }

  /**
   * Creates a command which takes over the fields <code>_type</code>, <code>_id</code> and <code>
   * _jwt</code> from <code>json</code>.
   *
   * @param json the aggregate or event.
   * @param name the name of the command.
   * @return The new command.
   * @since 1.0
   */
  public static JsonObject createCommandFor(final JsonObject json, final String name) {
    return createCommandBuilderFor(json, name).build();
  }

  /**
   * Creates a command builder which takes over the fields <code>_type</code>, <code>_id</code> and
   * <code>
   * _jwt</code> from <code>json</code>.
   *
   * @param json the aggregate or event.
   * @param name the name of the command.
   * @return The new command.
   * @since 1.0
   */
  public static JsonObjectBuilder createCommandBuilderFor(
      final JsonObject json, final String name) {
    return create(() -> createCommandBuilder(json.getString(TYPE), json.getString(ID), name))
        .updateIf(
            builder -> json.containsKey(JWT), builder -> builder.add(JWT, json.getJsonObject(JWT)))
        .build();
  }

  /**
   * Checks if <code>json</code> is marked with an error, which means the field <code>_error</code>
   * is set to <code>true</code>.
   *
   * @param json the given object, usually a command.
   * @return The check report.
   * @see JsonFields
   * @since 1.0
   */
  public static boolean hasError(final JsonObject json) {
    return json != null && json.getBoolean(ERROR, false);
  }

  /**
   * Checks if <code>json</code> is a command. This means it has the fields <code>_id</code>, <code>
   * _type</code> and <code>_command</code>.
   *
   * @param json the given object.
   * @return The check report.
   * @see JsonFields
   * @since 1.0
   */
  public static boolean isCommand(final JsonObject json) {
    return json != null
        && json.containsKey(ID)
        && json.containsKey(CORR)
        && json.containsKey(TYPE)
        && json.containsKey(COMMAND)
        && !json.containsKey(OPS);
  }

  /**
   * Checks if the <code>command</code> has the <code>name</code>.
   *
   * @param command the given command.
   * @param name the name to be checked.
   * @return The check report.
   * @since 1.0
   */
  public static boolean isCommand(final JsonObject command, final String name) {
    return isCommand(command) && name != null && name.equals(command.getString(COMMAND, ""));
  }
}
