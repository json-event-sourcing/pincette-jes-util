package net.pincette.jes.util;

import static com.fasterxml.jackson.dataformat.cbor.CBORFactory.builder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.json.stream.JsonParser.Event.START_OBJECT;
import static net.pincette.util.Json.from;
import static net.pincette.util.Util.tryToGetWithSilent;

import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.stream.JsonParser;
import net.pincette.jf.JacksonParser;
import net.pincette.jf.JsonParserWrapper;
import net.pincette.util.Json;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Deserializes JSON objects from compressed CBOR or strings.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class JsonDeserializer implements Deserializer<JsonObject> {
  private final CBORFactory factory = builder().build();

  private static JsonObject getObject(final JsonParser parser) {
    if (parser.next() != START_OBJECT) {
      throw new IllegalStateException("Not an object");
    }

    return parser.getObject();
  }

  private static Optional<JsonObject> parse(final byte[] bytes) {
    return from(new String(bytes, UTF_8)).filter(Json::isObject).map(JsonValue::asJsonObject);
  }

  @Override
  public void close() {
    // Nothing to close.
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean isKey) {
    // Nothing to configure.
  }

  public JsonObject deserialize(final String topic, final byte[] bytes) {
    return Optional.ofNullable(bytes)
        .map(b -> fromCbor(b).orElseGet(() -> parse(bytes).orElse(null)))
        .orElse(null);
  }

  private Optional<JsonObject> fromCbor(final byte[] bytes) {
    return tryToGetWithSilent(
        () ->
            new JsonParserWrapper(
                new JacksonParser(
                    factory.createParser(new GZIPInputStream(new ByteArrayInputStream(bytes))))),
        JsonDeserializer::getObject);
  }
}
