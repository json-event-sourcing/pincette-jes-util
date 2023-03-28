package net.pincette.jes.util;

import static com.typesafe.config.ConfigFactory.parseFile;
import static com.typesafe.config.ConfigFactory.parseResources;
import static java.lang.System.getProperty;
import static java.util.Optional.ofNullable;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetSilent;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;

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
        .flatMap(file -> tryToGetRethrow(() -> parseFile(new File(file))))
        .orElse(null);
  }

  private static Config asResource() {
    return ofNullable(getProperty("config.resource"))
        .flatMap(resource -> tryToGetRethrow(() -> parseFile(new File(new File("conf"), resource))))
        .orElse(null);
  }

  private static Config asSystemResource() {
    return tryToGetSilent(() -> parseResources(Configuration.class, "/conf/application.conf"))
        .orElse(null);
  }

  private static Config defaultConfig() {
    return tryToGetSilent(() -> parseFile(new File(new File("conf"), "application.conf")))
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
        .or(Configuration::asSystemResource)
        .or(Configuration::defaultConfig)
        .get()
        .orElseGet(ConfigFactory::load);
  }
}
