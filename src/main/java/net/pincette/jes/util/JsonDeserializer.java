package net.pincette.jes.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static net.pincette.util.Json.from;

import java.util.Map;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.util.Json;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Deserializes JSON objects from strings.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class JsonDeserializer implements Deserializer<JsonObject> {
  @Override
  public void close() {
    // Nothing to close.
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean isKey) {
    // Nothing to configure.
  }

  public JsonObject deserialize(final String topic, final byte[] bytes) {
    return bytes != null
        ? from(new String(bytes, UTF_8))
            .filter(Json::isObject)
            .map(JsonValue::asJsonObject)
            .orElse(null)
        : null;
  }
}
