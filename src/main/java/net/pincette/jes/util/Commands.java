package net.pincette.jes.util;

/**
 * Built-in command names.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Commands {
  /**
   * Logical delete of an aggregate instance.
   *
   * @since 1.0
   */
  public static final String DELETE = "delete";

  /**
   * Gets an aggregate.
   *
   * @since 1.0
   */
  public static final String GET = "get";

  /**
   * Applies a JSON patch.
   *
   * @since 1.0
   */
  public static final String PATCH = "patch";

  /**
   * Replaces the state of an aggregate instance.
   *
   * @since 1.0
   */
  public static final String PUT = "put";

  private Commands() {}
}
