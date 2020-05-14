package net.pincette.jes.util;

import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createValue;
import static javax.json.JsonValue.NULL;
import static net.pincette.jes.util.Mongo.collection;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.Expression.memberFunction;
import static net.pincette.mongo.Expression.registerExtension;
import static net.pincette.util.Util.tryToGetRethrow;

import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.Implementation;
import net.pincette.mongo.Operator;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * This is a set of MongoDB operator extensions.
 *
 * @author Werner Donn\u00e9
 * @since 1.3
 */
public class MongoExpressions {
  private static final String APP_FIELD = "app";
  private static final String FIND_ONE_OP = "$jes-findOne";
  private static final String FIND_OP = "$jes-find";
  private static final String HREF_OP = "$jes-href";
  private static final String ID_FIELD = "id";
  private static final String QUERY_FIELD = "query";
  private static final String TYPE_FIELD = "type";

  private MongoExpressions() {}

  /**
   * This extension is called <code>$jes-find</code>. Its expression is an object with the fields
   * <code>app</code>, <code>type</code> and <code>query</code>. All are mandatory. The
   * subexpressions <code>app</code> and <code>type</code>should generate a string. The <code>query
   * </code> subexpression should be an object that represents a valid MongoDB query operator. The
   * calculated value is a JSON array.
   *
   * @param database the MongoDB database.
   * @param environment the environment.
   * @return The implementation.
   * @since 1.3
   */
  public static Operator find(final MongoDatabase database, final String environment) {
    return expression -> finder(expression, database, environment, MongoExpressions::findArray);
  }

  private static CompletionStage<JsonValue> findArray(
      final MongoCollection<Document> collection, final Bson filter) {
    return Mongo.find(collection, filter)
        .thenApply(
            r ->
                r.stream()
                    .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
                    .build());
  }

  private static CompletionStage<JsonValue> findObject(
      final MongoCollection<Document> collection, final Bson filter) {
    return Mongo.findOne(collection, filter).thenApply(r -> r.map(j -> (JsonValue) j).orElse(NULL));
  }

  /**
   * This extension is called <code>$jes-findOne</code>. Its expression is an object with the fields
   * <code>app</code>, <code>type</code> and <code>query</code>. All are mandatory. The
   * subexpressions <code>app</code> and <code>type</code>should generate a string. The <code>query
   * </code> subexpression should be an object that represents a valid MongoDB query operator. The
   * calculated value is a JSON object or <code>NULL</code> if it doesn't exist.
   *
   * @param database the MongoDB database.
   * @param environment the environment.
   * @return The implementation.
   * @since 1.3
   */
  public static Operator findOne(final MongoDatabase database, final String environment) {
    return expression -> finder(expression, database, environment, MongoExpressions::findObject);
  }

  private static Implementation finder(
      final JsonValue expression,
      final MongoDatabase database,
      final String environment,
      final BiFunction<MongoCollection<Document>, Bson, CompletionStage<JsonValue>> op) {
    final Implementation app = memberFunction(expression, APP_FIELD);
    final Implementation query = memberFunction(expression, QUERY_FIELD);
    final Implementation type = memberFunction(expression, TYPE_FIELD);

    return (json, vars) ->
        app != null && query != null && type != null
            ? Optional.of(query.apply(json, vars))
                .filter(JsonUtil::isObject)
                .map(JsonValue::asJsonObject)
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
                                        fromJson(value))
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
   * @return The implementation.
   * @since 1.3
   */
  public static Implementation href(final JsonValue expression) {
    final Implementation app = memberFunction(expression, APP_FIELD);
    final Implementation id = memberFunction(expression, ID_FIELD);
    final Implementation type = memberFunction(expression, TYPE_FIELD);

    return (json, vars) ->
        app != null && type != null
            ? createValue(
                new Href(string(app, json, vars), string(type, json, vars), string(id, json, vars))
                    .path())
            : NULL;
  }

  /**
   * This globally registers all the extensions of this class. It should be called only once.
   *
   * @param database the MongoDB database.
   * @param environment the environment.
   * @since 1.3
   */
  public static void register(final MongoDatabase database, final String environment) {
    registerExtension(FIND_ONE_OP, findOne(database, environment));
    registerExtension(FIND_OP, find(database, environment));
    registerExtension(HREF_OP, MongoExpressions::href);
  }

  private static String string(
      final Implementation implementation,
      final JsonObject json,
      final Map<String, JsonValue> variables) {
    return implementation != null
        ? asString(implementation.apply(json, variables)).getString()
        : null;
  }
}
