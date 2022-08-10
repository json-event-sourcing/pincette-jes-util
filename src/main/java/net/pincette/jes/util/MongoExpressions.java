package net.pincette.jes.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.UUID.nameUUIDFromBytes;
import static java.util.UUID.randomUUID;
import static javax.json.JsonValue.FALSE;
import static javax.json.JsonValue.NULL;
import static net.pincette.jes.util.Mongo.collection;
import static net.pincette.json.JsonUtil.asLong;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.mongo.Expression.implementation;
import static net.pincette.mongo.Expression.memberFunction;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.tryToGetRethrow;

import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import net.pincette.jes.Event;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.Features;
import net.pincette.mongo.Implementation;
import net.pincette.mongo.JsonClient;
import net.pincette.mongo.Operator;
import org.bson.Document;

/**
 * This is a set of MongoDB operator extensions.
 *
 * @author Werner Donn\u00e9
 * @since 1.3
 */
public class MongoExpressions {
  private static final String ADDED = "$jes-added";
  private static final String APP_FIELD = "app";
  private static final String CHANGED = "$jes-changed";
  private static final String FIND_ONE_OP = "$jes-findOne";
  private static final String FIND_OP = "$jes-find";
  private static final String FROM = "from";
  private static final String HREF_OP = "$jes-href";
  private static final String ID_FIELD = "id";
  private static final String KEY = "key";
  private static final String NAME_UUID = "$jes-name-uuid";
  private static final String POINTER = "pointer";
  private static final String QUERY_FIELD = "query";
  private static final String REMOVED = "$jes-removed";
  private static final String SCOPE = "scope";
  private static final String TO = "to";
  private static final String TYPE_FIELD = "type";
  private static final String UUID = "$jes-uuid";

  private MongoExpressions() {}

  /**
   * This extension is called <code>$jes-added</code>. Its expression is an object with the field
   * <code>pointer</code>. It should be an expression that yields a JSON pointer. The operator
   * returns <code>true</code> if the pointed field was added in the event.
   *
   * @param expression the given expression.
   * @param features extra features for expression implementations.
   * @return The implementation.
   * @since 1.4.1
   */
  public static Implementation added(final JsonValue expression, final Features features) {
    return changedSimple(expression, features, Event::added);
  }

  /**
   * This extension is called <code>$jes-changed</code>. Its expression is an object with the fields
   * <code>pointer</code>, <code>from</code> and <code>to</code>. Only the first field is mandatory.
   * It should be an expression that yields a JSON pointer. The operator returns <code>true</code>
   * if the pointed field has changed in the event. If both the <code>from</code> and <code>to
   * </code> fields are present then the operator returns <code>true</code> when the pointed field
   * in the event has transitioned between the two values produced by their expressions. In that
   * case the event should be a full event, which has the <code>_before</code> field.
   *
   * @param expression the given expression.
   * @param features extra features for expression implementations.
   * @return The implementation.
   * @since 1.3.2
   */
  public static Implementation changed(final JsonValue expression, final Features features) {
    final Implementation from = memberFunction(expression, FROM, features);
    final Implementation pointer = memberFunction(expression, POINTER, features);
    final Implementation to = memberFunction(expression, TO, features);

    return (json, vars) ->
        pointer != null
            ? pointer(pointer, json, vars)
                .map(
                    p ->
                        createValue(
                            (from != null
                                    && to != null
                                    && Event.changed(
                                        json, p, from.apply(json, vars), to.apply(json, vars)))
                                || ((from == null || to == null) && Event.changed(json, p))))
                .orElse(FALSE)
            : NULL;
  }

  private static Implementation changedSimple(
      final JsonValue expression,
      final Features features,
      final BiPredicate<JsonObject, String> op) {
    final Implementation pointer = implementation(expression, features);

    return (json, vars) ->
        pointer(pointer, json, vars).map(p -> createValue(op.test(json, p))).orElse(FALSE);
  }

  /**
   * This extension is called <code>$jes-find</code>. Its expression is an object with the fields
   * <code>app</code>, <code>type</code> and <code>query</code>. All are mandatory. The
   * subexpressions <code>app</code> and <code>type</code>should generate a string. The <code>query
   * </code> subexpression should be an object that represents a valid MongoDB query operator or an
   * aggregation pipeline. The calculated value is a JSON array.
   *
   * @param database the MongoDB database.
   * @param environment the environment.
   * @return The implementation.
   * @since 1.3
   */
  public static Operator find(final MongoDatabase database, final String environment) {
    return (expression, features) ->
        finder(expression, database, environment, MongoExpressions::findArray, features);
  }

  private static CompletionStage<JsonValue> findArray(
      final MongoCollection<Document> collection, final JsonStructure query) {
    return isObject(query)
        ? JsonClient.find(collection, query.asJsonObject())
            .thenApply(
                r ->
                    r.stream()
                        .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
                        .build())
        : JsonClient.aggregate(collection, query.asJsonArray()).thenApply(JsonUtil::from);
  }

  private static CompletionStage<JsonValue> findObject(
      final MongoCollection<Document> collection, final JsonStructure query) {
    final Function<List<JsonObject>, JsonValue> result =
        list -> list.size() == 1 ? list.get(0) : NULL;

    return isObject(query)
        ? JsonClient.findOne(collection, query.asJsonObject())
            .thenApply(r -> r.map(JsonValue.class::cast).orElse(NULL))
        : JsonClient.aggregate(collection, query.asJsonArray()).thenApply(result);
  }

  /**
   * This extension is called <code>$jes-findOne</code>. Its expression is an object with the fields
   * <code>app</code>, <code>type</code> and <code>query</code>. All are mandatory. The
   * subexpressions <code>app</code> and <code>type</code> should generate a string. The <code>query
   * </code> subexpression should be an object that represents a valid MongoDB query operator or an
   * aggregation pipeline. The calculated value is a JSON object or <code>NULL</code> if it doesn't
   * exist.
   *
   * @param database the MongoDB database.
   * @param environment the environment.
   * @return The implementation.
   * @since 1.3
   */
  public static Operator findOne(final MongoDatabase database, final String environment) {
    return (expression, features) ->
        finder(expression, database, environment, MongoExpressions::findObject, features);
  }

  private static Implementation finder(
      final JsonValue expression,
      final MongoDatabase database,
      final String environment,
      final BiFunction<MongoCollection<Document>, JsonStructure, CompletionStage<JsonValue>> op,
      final Features features) {
    final Implementation app = memberFunction(expression, APP_FIELD, features);
    final Implementation query = memberFunction(expression, QUERY_FIELD, features);
    final Implementation type = memberFunction(expression, TYPE_FIELD, features);

    return (json, vars) ->
        app != null && query != null && type != null
            ? Optional.of(query.apply(json, vars))
                .filter(value -> isObject(value) || isArray(value))
                .flatMap(
                    value ->
                        tryToGetRethrow(
                            () ->
                                op.apply(
                                        database.getCollection(
                                            collection(
                                                new Href(
                                                    string(app, json, vars),
                                                    string(type, json, vars)),
                                                environment)),
                                        (JsonStructure) value)
                                    .toCompletableFuture()
                                    .get()))
                .orElse(NULL)
            : NULL;
  }

  /**
   * This extension is called <code>$jes-href</code>. Its expression is an object with the fields
   * <code>app</code>, <code>type</code> and <code>id</code>. The latter is optional. All
   * subexpressions should generate a string. The calculated value is the href path.
   *
   * @param expression the given expression.
   * @param features extra features for expression implementations.
   * @return The implementation.
   * @since 1.3
   */
  public static Implementation href(final JsonValue expression, final Features features) {
    final Implementation app = memberFunction(expression, APP_FIELD, features);
    final Implementation id = memberFunction(expression, ID_FIELD, features);
    final Implementation type = memberFunction(expression, TYPE_FIELD, features);

    return (json, vars) ->
        app != null && type != null
            ? createValue(
                new Href(string(app, json, vars), string(type, json, vars), string(id, json, vars))
                    .path())
            : NULL;
  }

  /**
   * This extension is called <code>$jes-name-uuid</code>. Its expression is an object with the
   * mandatory fields <code>scope</code> and <code>key</code>. The expression of the former should
   * yield a string and that of the latter an integer.
   *
   * @param expression the given expression.
   * @param features extra features for expression implementations.
   * @return The implementation.
   * @since 1.3.1
   */
  public static Implementation nameUUID(final JsonValue expression, final Features features) {
    final Implementation scope = memberFunction(expression, SCOPE, features);
    final Implementation key = memberFunction(expression, KEY, features);

    return (json, vars) ->
        scope != null && key != null
            ? createValue(
                nameUUIDFromBytes(
                        (asString(scope.apply(json, vars)).getString()
                                + "#"
                                + asLong(key.apply(json, vars)))
                            .getBytes(UTF_8))
                    .toString()
                    .toLowerCase())
            : NULL;
  }

  /**
   * Returns the JES extension operators for the MongoDB aggregation expression language.
   *
   * @param database the MongoDB database.
   * @param environment the environment.
   * @return The operator map.
   * @since 1.3.2
   */
  public static Map<String, Operator> operators(
      final MongoDatabase database, final String environment) {
    return map(
        pair(ADDED, MongoExpressions::added),
        pair(CHANGED, MongoExpressions::changed),
        pair(FIND_ONE_OP, findOne(database, environment)),
        pair(FIND_OP, find(database, environment)),
        pair(HREF_OP, MongoExpressions::href),
        pair(NAME_UUID, MongoExpressions::nameUUID),
        pair(REMOVED, MongoExpressions::removed),
        pair(UUID, (e, f) -> uuid()));
  }

  private static Optional<String> pointer(
      final Implementation fn, final JsonObject json, final Map<String, JsonValue> vars) {
    return Optional.of(fn.apply(json, vars))
        .filter(JsonUtil::isString)
        .map(JsonUtil::asString)
        .map(JsonString::getString);
  }

  /**
   * This extension is called <code>$jes-removed</code>. Its expression is an object with the field
   * <code>pointer</code>. It should be an expression that yields a JSON pointer. The operator
   * returns <code>true</code> if the pointed field was removed in the event.
   *
   * @param expression the given expression.
   * @param features extra features for expression implementations.
   * @return The implementation.
   * @since 1.4.1
   */
  public static Implementation removed(final JsonValue expression, final Features features) {
    return changedSimple(expression, features, Event::removed);
  }

  private static String string(
      final Implementation implementation,
      final JsonObject json,
      final Map<String, JsonValue> variables) {
    return implementation != null
        ? asString(implementation.apply(json, variables)).getString()
        : null;
  }

  /**
   * This extension is called <code>$jes-uuid</code>. The operator produces a UUID.
   *
   * @return The implementation.
   * @since 1.3.1
   */
  public static Implementation uuid() {
    return (json, vars) -> createValue(randomUUID().toString().toLowerCase());
  }
}
