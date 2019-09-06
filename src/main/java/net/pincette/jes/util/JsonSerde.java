package net.pincette.jes.util;

import java.util.Map;
import javax.json.JsonObject;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A Kafka serde for JSON objects.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class JsonSerde implements Serde<JsonObject> {
  @Override
  public void close() {
    // Nothing to close.
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean isKey) {
    // Nothing to configure.
  }

  public Deserializer<JsonObject> deserializer() {
    return new JsonDeserializer();
  }

  public Serializer<JsonObject> serializer() {
    return new JsonSerializer();
  }
}
