package net.pincette.jes.util;

import static net.pincette.util.Util.getSegments;
import static net.pincette.util.Util.tryToGetRethrow;

import java.net.URI;
import java.util.Optional;
import net.pincette.util.Util.GeneralException;

/**
 * A utility to work with hrefs in aggregates.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Href {
  private static String contextPath;
  public final String app;
  public final String id;
  public final String type;

  /**
   * If the <code>type</code> is not prefixed it will be prefixed with the value of <code>app</code>
   * and a dash.
   *
   * @param app the given app name.
   * @param type the given aggregate type.
   * @since 1.0
   */
  public Href(final String app, final String type) {
    this(app, type, null);
  }

  /**
   * If the <code>type</code> is not prefixed it will be prefixed with the value of <code>app</code>
   * and a dash.
   *
   * @param app the given app name.
   * @param type the given aggregate type.
   * @param id the given aggregate instance ID.
   * @since 1.0
   */
  public Href(final String app, final String type, final String id) {
    this.app = app;
    this.type = addPrefix(app, type);
    this.id = id;
  }

  /**
   * Splits <code>href</code> in app, type and id. If the type is not prefixed it will be prefixed
   * with the value of app and a dash.
   *
   * @param href must be or have the URI path of the form described in {@link #path()}.
   * @since 1.0
   */
  public Href(final String href) {
    this(getPath(href)[0], getPath(href)[1], getId(href));
  }

  private static String addPrefix(final String app, final String type) {
    return Optional.of(type.indexOf('-'))
        .filter(index -> index == -1)
        .map(index -> app + "-" + type)
        .orElse(type);
  }

  private static String getHref(final String href) {
    return contextPath != null ? href.substring(contextPath.length()) : href;
  }

  private static String getId(final String href) {
    return Optional.of(getPath(href))
        .filter(path -> path.length == 3)
        .map(path -> path[2])
        .orElse(null);
  }

  private static String[] getPath(final String href) {
    final String[] path = split(getHref(href));

    if (path.length < 2 || path.length > 3) {
      throw new GeneralException("Invalid href " + href);
    }

    return path;
  }

  private static String removePrefix(final String type) {
    return Optional.of(type.indexOf('-'))
        .filter(index -> index != -1)
        .map(index -> type.substring(index + 1))
        .orElse(type);
  }

  /**
   * Sets the global context path, which will be prepended to the path. This way an application can
   * retrieve the context path from a configuration and set it once.
   *
   * @param path the context path.
   * @since 1.0.1
   */
  public static void setContextPath(final String path) {
    contextPath = path;
  }

  private static String[] split(final String href) {
    return getSegments(
            href.startsWith("/")
                ? href
                : tryToGetRethrow(() -> new URI(href)).map(URI::getPath).orElse(""),
            "/")
        .toArray(String[]::new);
  }

  /**
   * Generates the path for the href, which has the form /&lt;app&gt;/&lt;type&gt;[/&lt;id&gt;. The
   * type will appear without prefix. If the global context path is not <code>null</code> it will be
   * prepended to the path.
   *
   * @return The path.
   * @since 1.0
   */
  public String path() {
    return (contextPath != null ? contextPath : "")
        + "/"
        + app
        + "/"
        + removePrefix(type)
        + (id != null ? ("/" + id) : "");
  }
}
