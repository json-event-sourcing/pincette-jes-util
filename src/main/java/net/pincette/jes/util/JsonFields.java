package net.pincette.jes.util;

/**
 * Standard fields for aggregates, events and commands. @@author Werner Donn\u00e9
 *
 * @since 1.0
 */
public class JsonFields {
  /**
   * The access control list field, which refers to an object with a <code>read</code> and a <code>
   * write</code> field, both of which are arrays with principal names.
   *
   * @since 1.0
   */
  public static final String ACL = "_acl";

  /**
   * The <code>get</code> field of an ACL, which represents the <code>get</code> command.
   *
   * @since 1.0
   */
  public static final String ACL_GET = "get";

  /**
   * The <code>write</code> field of an ACL. This field is used as a fallback for commands that
   * aren't mentioned in the ACL.
   *
   * @since 1.0
   */
  public static final String ACL_WRITE = "write";

  /**
   * The aggregate after the reduction phase. It is present on published events, but it is stripped
   * from the event when it is saved in the event log.
   *
   * @since 1.0
   */
  public static final String AFTER = "_after";

  /**
   * The aggregate before the reduction phase. It is present on published events, but it is stripped
   * from the event when it is saved in the event log.
   *
   * @since 1.0
   */
  public static final String BEFORE = "_before";

  /**
   * The command name.
   *
   * @since 1.0
   */
  public static final String COMMAND = "_command";

  /**
   * The correlation ID of a request. It is propagated from commands to events and aggregates. This
   * way the entire event flow can be followed. A correlation ID will be generated if the client
   * hasn't set it in the submitted command.
   *
   * @since 1.0
   */
  public static final String CORR = "_corr";

  /**
   * Indicates if an aggregate has been deleted. Aggregates are not physically destroyed.
   *
   * @since 1.0
   */
  public static final String DELETED = "_deleted";

  /**
   * This boolean field indicates validation errors in commands. Commands with such errors are sent
   * back to the client.
   *
   * @since 1.0
   */
  public static final String ERROR = "_error";

  /**
   * The UUID of an aggregate instance.
   *
   * @since 1.0
   */
  public static final String ID = "_id";

  /**
   * The payload of the JSON Web Token that was presented by the user in the REST API call. It is
   * set on submitted commands and propagates to events and aggregates.
   *
   * @since 1.0
   */
  public static final String JWT = "_jwt";

  /**
   * The <code>breakingTheGlass</code> field in the JWT payload. This overrules ACL checking.
   *
   * @since 1.0
   */
  public static final String JWT_BREAKING_THE_GLASS = "breakingTheGlass";

  /**
   * The lock object, which contains the <code>sub</code> and <code>time</code> fields. The time *
   * is in milliseconds.
   *
   * @since 1.1.4
   */
  public static final String LOCK = "_lock";

  /**
   * The <code>roles</code> field in the JWT payload.
   *
   * @since 1.0
   * @deprecated Use the <code>ROLES</code> field.
   */
  @Deprecated public static final String JWT_ROLES = "roles";

  /**
   * The mandatory <code>sub</code> field in the JWT payload.
   *
   * @since 1.0
   * @deprecated Use the <code>SUB</code> field.
   */
  @Deprecated public static final String JWT_SUB = "sub";

  /**
   * An array of language tags in the order of preference, which can be set on a command. When a
   * validator or some other component wishes to send messages to the user, it can use the proper
   * language for it.
   *
   * @since 1.0
   */
  public static final String LANGUAGES = "_languages";

  /**
   * An array of operations as described in RFC 6902. Events always have this field. It describes
   * how an aggregate instance has changed after the reduction of a command.
   *
   * @since 1.0
   */
  public static final String OPS = "_ops";

  /**
   * The array of strings that can appear in the JWT payload.
   *
   * @since 1.1.4
   */
  public static final String ROLES = "_roles";

  /**
   * The <code>sub</code> field, which appears in the JWT payload, the lock object and the
   * subscription objects.
   *
   * @since 1.1.4
   */
  public static final String SUB = "sub";

  /**
   * The sequence number of an event. When an aggregate instance is fetched it will contain the
   * sequence number of the last event that was generated from it.
   *
   * @since 1.0
   */
  public static final String SEQ = "_seq";

  /**
   * An HTTP status code that may be set on rejected commands.*
   *
   * @since 1.0
   */
  public static final String STATUS_CODE = "_statusCode";

  /**
   * The array of subscription objects, which contain the <code>sub</code> and <code>time</code>
   * fields.
   *
   * @since 1.1.4
   */
  public static final String SUBSCRIPTIONS = "_subscriptions";

  /**
   * This boolean field sets the test mode, in which case responses of validation errors and
   * aggregate updates go back to the HTTP response body instead of being sent asynchronously.
   *
   * @since 1.0
   */
  public static final String TEST = "_test";

  /**
   * A timestamp marker for commands an events.*
   *
   * @since 1.0
   */
  public static final String TIMESTAMP = "_timestamp";

  /**
   * The aggregate type, which must be unique within a cluster.*
   *
   * @since 1.0
   */
  public static final String TYPE = "_type";

  private JsonFields() {}
}
