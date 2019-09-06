package net.pincette.jes.util;

import static com.typesafe.config.ConfigFactory.load;
import static java.lang.System.getProperty;
import static net.pincette.util.Util.tryToGetSilent;
import static net.pincette.util.Util.tryToGetWithRethrow;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Optional;

/**
 * Some configuration utilities.
 *
 * @since 1.0
 * @author Werner Donn\u00e9
 */
public class Configuration {
  private Configuration() {}

  /**
   * Tries to load the default configuration <code>application.conf</code>from the <code>conf</code>
   * directory. If the system property <code>config.resource</code> is set its value is also
   * resolved against the <code>conf</code> directory.
   *
   * @return The resolved configuration.
   * @see <a
   *     href="http://lightbend.github.io/config/latest/api/com/typesafe/config/ConfigFactory.html#load-java.lang.ClassLoader-">load</a>
   * @since 1.0
   */
  public static Config loadDefault() {
    return tryToGetWithRethrow(
            () ->
                new URLClassLoader(
                    new URL[] {
                      tryToGetSilent(() -> new File("conf/").toURI().toURL()).orElse(null)
                    }),
            classLoader ->
                Optional.ofNullable(getProperty("config.resource"))
                    .map(config -> load(classLoader, config))
                    .orElseGet(() -> load(classLoader)))
        .orElseGet(ConfigFactory::empty);
  }
}
