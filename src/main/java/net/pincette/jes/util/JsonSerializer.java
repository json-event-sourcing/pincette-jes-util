package net.pincette.jes.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static net.pincette.util.Json.string;

import java.util.Map;
import javax.json.JsonObject;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serializes JSON objects to strings.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class JsonSerializer implements Serializer<JsonObject> {
  @Override
  public void close() {
    // Nothing to close.
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean isKey) {
    // Nothing to configure.
  }

  public byte[] serialize(final String topic, final JsonObject json) {
    return json != null ? string(json).getBytes(UTF_8) : null;
  }
}
