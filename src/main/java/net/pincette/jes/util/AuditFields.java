package net.pincette.jes.util;

/**
 * The JSON fields for audit records.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class AuditFields {
  /**
   * The ID of the aggregate instance.
   *
   * @since 1.0
   */
  public static final String AGGREGATE = "aggregate";

  /**
   * The boolean field that indicates whether or not "breaking the glass" was applied.
   *
   * @since 1.0
   */
  public static final String BREAKING_THE_GLASS = "breakingTheGlass";

  /**
   * The name of the command that was issued.
   *
   * @since 1.0
   */
  public static final String COMMAND = "command";

  /**
   * The timestamp when the command occurred.
   *
   * @since 1.0
   */
  public static final String TIMESTAMP = "timestamp";

  /**
   * The aggregate type.
   *
   * @since 1.0
   */
  public static final String TYPE = "type";

  /**
   * The username of the user who issued the command.
   *
   * @since 1.0
   */
  public static final String USER = "user";

  private AuditFields() {}
}
