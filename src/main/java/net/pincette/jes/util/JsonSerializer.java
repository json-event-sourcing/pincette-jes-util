package net.pincette.jes.util;

import static com.fasterxml.jackson.dataformat.cbor.CBORFactory.builder;
import static net.pincette.util.Util.tryToDoRethrow;

import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Optional;
import java.util.zip.GZIPOutputStream;
import javax.json.JsonObject;
import net.pincette.json.filter.JacksonGenerator;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serializes JSON objects to compressed CBOR.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class JsonSerializer implements Serializer<JsonObject> {
  private final CBORFactory factory = builder().build();

  @Override
  public void close() {
    // Nothing to close.
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean isKey) {
    // Nothing to configure.
  }

  public byte[] serialize(final String topic, final JsonObject json) {
    return Optional.ofNullable(json).map(this::toCompressedCbor).orElse(null);
  }

  private byte[] toCompressedCbor(final JsonObject json) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    tryToDoRethrow(
        () ->
            new JacksonGenerator(factory.createGenerator(new GZIPOutputStream(out)))
                .write(json)
                .close());

    return out.toByteArray();
  }
}
