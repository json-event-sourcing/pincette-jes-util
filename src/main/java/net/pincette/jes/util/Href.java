package net.pincette.jes.util;

import static java.util.Objects.hash;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.getSegments;
import static net.pincette.util.Util.isUUID;
import static net.pincette.util.Util.tryToGetRethrow;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import net.pincette.util.Cases;
import net.pincette.util.Pair;
import net.pincette.util.Util.GeneralException;

/**
 * A utility to work with hrefs in aggregates.
 *
 * @author Werner Donné
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
   * @param id the given aggregate instance ID. It may be <code>null</code>.
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
    final Href h = decompose(href).orElseThrow(() -> new GeneralException("Invalid href " + href));

    id = h.id;
    app = h.app;
    type = addPrefix(h.app, h.type);
  }

  private static String addPrefix(final String app, final String type) {
    return Optional.of(type.indexOf('-'))
        .filter(index -> index == -1)
        .map(index -> app + "-" + type)
        .orElse(type);
  }

  private static Optional<Href> decompose(final String path) {
    return Cases.<String[], Href>withValue(
            split(path.substring(contextPath != null ? contextPath.length() : 0)))
        .or(p -> p.length == 3 && isUUID(p[2]), p -> new Href(p[0], p[1], p[2]))
        .or(
            p -> p.length == 2 && isUUID(p[1]),
            p -> fullType(p[0]).map(pair -> new Href(pair.first, pair.second, p[1])).orElse(null))
        .or(p -> p.length == 2 && !isUUID(p[1]), p -> new Href(p[0], p[1]))
        .or(
            p -> p.length == 1,
            p -> fullType(p[0]).map(pair -> new Href(pair.first, pair.second)).orElse(null))
        .get();
  }

  private static Optional<Pair<String, String>> fullType(final String s) {
    return Optional.of(s.indexOf('-'))
        .filter(i -> i != -1)
        .map(i -> pair(s.substring(0, i), s.substring(i + 1)));
  }

  public static boolean isHref(final String path) {
    return decompose(path).isPresent();
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
    if (contextPath != null) {
      throw new GeneralException("The href context path can be set only once.");
    }

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

  @Override
  public boolean equals(final Object other) {
    return other instanceof Href href
        && href.app.equals(app)
        && href.type.equals(type)
        && Objects.equals(href.id, id);
  }

  @Override
  public int hashCode() {
    return hash(app, id, type);
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
