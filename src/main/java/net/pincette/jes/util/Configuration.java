package net.pincette.jes.util;

import static com.typesafe.config.ConfigFactory.parseReader;
import static java.lang.System.getProperty;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.ofNullable;
import static net.pincette.util.Or.tryWith;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Optional;

/**
 * Some configuration utilities.
 *
 * @since 1.0
 * @author Werner Donné
 */
public class Configuration {
  private Configuration() {}

  private static Config asFile() {
    return ofNullable(getProperty("config.file"))
        .map(File::new)
        .filter(File::canRead)
        .map(ConfigFactory::parseFile)
        .orElse(null);
  }

  private static Config asResource() {
    return ofNullable(getProperty("config.resource"))
        .map(resource -> new File(new File("conf"), resource))
        .filter(File::canRead)
        .map(ConfigFactory::parseFile)
        .orElse(null);
  }

  private static Config asSystemResource() {
    return ofNullable(Configuration.class.getResourceAsStream("/conf/application.conf"))
        .map(in -> parseReader(new InputStreamReader(in, UTF_8)))
        .orElse(null);
  }

  private static Config defaultConfig() {
    return Optional.of(new File(new File("conf"), "application.conf"))
        .filter(File::canRead)
        .map(ConfigFactory::parseFile)
        .orElse(null);
  }

  /**
   * Tries to load the default configuration <code>application.conf</code> from the <code>conf
   * </code> directory. If the system property <code>config.resource</code> is set, its value is
   * also resolved against the <code>conf</code> directory. If the system property <code>config
   * .file</code> is set then the given file is loaded. If the system resource <code>
   * /conf/application.conf</code> is available, it will be loaded.
   *
   * @return The resolved configuration.
   * @see <a
   *     href="http://lightbend.github.io/config/latest/api/com/typesafe/config/ConfigFactory.html#load-java.lang.ClassLoader-">load</a>
   * @since 1.0
   */
  public static Config loadDefault() {
    return tryWith(Configuration::asFile)
        .or(Configuration::asResource)
        .or(Configuration::defaultConfig)
        .or(Configuration::asSystemResource)
        .get()
        .orElseGet(ConfigFactory::load);
  }
}
