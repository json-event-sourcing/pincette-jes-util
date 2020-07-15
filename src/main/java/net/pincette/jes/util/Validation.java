package net.pincette.jes.util;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.jes.util.JsonFields.ERROR;
import static net.pincette.jes.util.JsonFields.ERRORS;
import static net.pincette.jes.util.JsonFields.STATE;
import static net.pincette.json.JsonUtil.createObjectBuilder;

import java.util.Optional;
import java.util.function.Function;
import javax.json.JsonArray;
import javax.json.JsonObject;
import net.pincette.mongo.Validator;

public class Validation {
  private Validation() {}

  /**
   * The validator is generated from <code>source</code>, which can be a filename or a string that
   * starts with "resource:". In the latter case the specification will be loaded as a resource from
   * the class path. The validator can expect the <code>_state</code> field to be added to the
   * command. This way validation expressions can be written that take into account the current
   * state of the aggregate instance.
   *
   * @param source the source of the validation specification.
   * @return The generated validation reducer.
   * @since 1.3
   * @see Validator
   */
  public static Reducer validator(final String source, final Validator validators) {
    return validator(validators.validator(source));
  }

  /**
   * The validator can expect the <code>_state</code> field to be added to the command. This way
   * validation expressions can be written that take into account the current state of the aggregate
   * instance.
   *
   * @param validator the validator.
   * @return The generated validation reducer.
   * @since 1.3.2
   * @see Validator
   */
  public static Reducer validator(final Function<JsonObject, JsonArray> validator) {
    return (command, state) ->
        completedFuture(
            Optional.of(validator.apply(createObjectBuilder(command).add(STATE, state).build()))
                .filter(errors -> !errors.isEmpty())
                .map(
                    errors ->
                        createObjectBuilder(command).add(ERROR, true).add(ERRORS, errors).build())
                .orElse(command));
  }
}
