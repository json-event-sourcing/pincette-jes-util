package net.pincette.jes.util;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Sorts.ascending;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static net.pincette.jes.util.Command.isCommand;
import static net.pincette.jes.util.Event.applyEvent;
import static net.pincette.jes.util.Event.isEvent;
import static net.pincette.jes.util.JsonFields.CORR;
import static net.pincette.jes.util.JsonFields.DELETED;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.OPS;
import static net.pincette.jes.util.JsonFields.SEQ;
import static net.pincette.jes.util.JsonFields.TIMESTAMP;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.jes.util.Util.isManagedObject;
import static net.pincette.json.JsonUtil.add;
import static net.pincette.json.JsonUtil.copy;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.emptyObject;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.Transform.transform;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.Collection.deleteOne;
import static net.pincette.mongo.Collection.exec;
import static net.pincette.mongo.Patch.updateOperators;
import static net.pincette.rs.Reducer.reduce;
import static net.pincette.rs.Util.subscribe;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.composeAsyncStream;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.rethrow;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.json.Transform.JsonEntry;
import net.pincette.json.Transform.Transformer;
import net.pincette.mongo.JsonClient;
import net.pincette.rs.Mapper;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

/**
 * MongoDB utilities.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Mongo {
  public static final Bson NOT_DELETED = ne(DELETED, true);
  private static final String HREF = "href";
  private static final String RESOLVED = "_resolved";
  private static final String SET = "$set";

  private Mongo() {}

  /**
   * Add the criterion that the aggregate must not be marked as deleted, which means the field
   * <code>_deleted</code> is either absent or <code>false</code>.
   *
   * @param query the orginal MongoDB query.
   * @return The extended query.
   * @since 1.0
   */
  public static Bson addNotDeleted(final Bson query) {
    return and(list(query, NOT_DELETED));
  }

  static String collection(final JsonObject json, final String environment) {
    return json.getString(TYPE) + collectionInfix(json) + suffix(environment);
  }

  static String collection(final Href href, final String environment) {
    return href.type + suffix(environment);
  }

  private static String collectionInfix(final JsonObject json) {
    final Supplier<String> tryCommand = () -> isCommand(json) ? "-command" : "";

    return isEvent(json) ? "-event" : tryCommand.get();
  }

  private static String eventCollection(final String type, final String environment) {
    return type + "-event" + suffix(environment);
  }

  /**
   * Returns the events of an aggregate instance in chronological order.
   *
   * @param id the identifier of the aggregate.
   * @param type the aggregate type.
   * @param environment the environment in which this is run, e.g. "dev", "prd", etc.
   * @param database the database which contains the event log.
   * @return The aggregate instance events.
   * @since 1.2
   */
  public static Publisher<JsonObject> events(
      final String id, final String type, final String environment, final MongoDatabase database) {
    return JsonClient.aggregationPublisher(
        database.getCollection(eventCollection(type, environment)),
        list(match(regex(ID, "^" + id + ".*")), sort(ascending(ID))));
  }

  /**
   * Returns the events of an aggregate instance in chronological order, starting from the given
   * snapshot.
   *
   * @param snapshot the snapshot of th aggregate instance to start from.
   * @param environment the environment in which this is run, e.g. "dev", "prd", etc.
   * @param database the database which contains the event log.
   * @return The aggregate instance events.
   * @since 1.2
   */
  public static Publisher<JsonObject> events(
      final JsonObject snapshot, final String environment, final MongoDatabase database) {
    return JsonClient.aggregationPublisher(
        database.getCollection(eventCollection(snapshot.getString(TYPE), environment)),
        list(
            match(
                and(
                    regex(ID, "^" + snapshot.getString(ID) + ".*"),
                    gt(SEQ, snapshot.getInt(SEQ, -1)))),
            sort(ascending(ID))));
  }

  private static CompletionStage<Map<Href, JsonObject>> fetchHrefs(
      final Set<Href> hrefs, final String environment, final MongoDatabase database) {
    return composeAsyncStream(
            hrefs.stream()
                .map(
                    href ->
                        findHref(href, environment, database)
                            .thenApply(j -> pair(href, j.orElse(null)))))
        .thenApply(
            stream ->
                stream
                    .filter(pair -> pair.second != null)
                    .collect(toMap(pair -> pair.first, pair -> pair.second)));
  }

  /**
   * Fetches the aggregate denoted by <code>href</code>.
   *
   * @param href the given href.
   * @param environment the environment in which the aggregate lives, e.g. test, acceptance,
   *     production, etc. This is added as a suffix to the collection name.
   * @param database the MongoDB database.
   * @return The aggregate instance.
   * @since 1.1.3
   */
  public static CompletionStage<Optional<JsonObject>> findHref(
      final Href href, final String environment, final MongoDatabase database) {
    return JsonClient.findOne(
        database.getCollection(collection(href, environment)), eq(ID, href.id));
  }

  private static boolean hrefOnly(final JsonObject json) {
    return json.containsKey(HREF) && json.size() == 1;
  }

  private static Set<Href> hrefs(final Stream<JsonObject> json) {
    return json.flatMap(JsonUtil::nestedObjects)
        .filter(Mongo::hrefOnly)
        .map(j -> new Href(j.getString(HREF)))
        .collect(toSet());
  }

  /**
   * Inserts the <code>collection</code> with <code>json</code>.
   *
   * @param json the given JSON Event Sourcing object, which can be an aggregate, event or a
   *     command.
   * @param collection the name of the collection. This may be <code>null</code>.
   * @param database the MongoDB database.
   * @return Whether the update was successful or not.
   * @since 1.1.2
   */
  public static CompletionStage<Boolean> insert(
      final JsonObject json, final String collection, final MongoDatabase database) {
    return JsonClient.insert(database.getCollection(collection), json);
  }

  /**
   * Inserts the <code>collection</code> with <code>json</code>.
   *
   * @param json the given JSON Event Sourcing object, which can be an aggregate, event or a
   *     command.
   * @param collection the name of the collection.
   * @param database the MongoDB database.
   * @param session the MongoDB session.
   * @return Whether the update was successful or not.
   * @since 1.1.2
   */
  public static CompletionStage<Boolean> insert(
      final JsonObject json,
      final String collection,
      final MongoDatabase database,
      final ClientSession session) {
    return JsonClient.insert(database.getCollection(collection), json, session);
  }

  /**
   * Inserts <code>json</code> in the collection of which the name is <code>
   * &lt;aggregate-type&gt;-(event-|command-|-)&lt;environment&gt;</code>. The identifier in the
   * collection is the identifier of the JSON Event Sourcing object.
   *
   * @param json the given JSON Event Sourcing object, which can be an aggregate, event or a
   *     command.
   * @param environment the environment in which the aggregate lives, e.g. test, acceptance,
   *     production, etc. This is added as a suffix to the collection name.
   * @param database the MongoDB database.
   * @return Whether the update was successful or not.
   * @since 1.1.2
   */
  public static CompletionStage<Boolean> insertJson(
      final JsonObject json, final String environment, final MongoDatabase database) {
    return insertJson(json, environment, database, null);
  }

  /**
   * Inserts <code>json</code> in the collection of which the name is <code>
   * &lt;aggregate-type&gt;-(event-|command-|-)&lt;environment&gt;</code>. The identifier in the
   * collection is the identifier of the JSON Event Sourcing object.
   *
   * @param json the given JSON Event Sourcing object, which can be an aggregate, event or a
   *     command.
   * @param environment the environment in which the aggregate lives, e.g. test, acceptance,
   *     production, etc. This is added as a suffix to the collection name.
   * @param database the MongoDB database.
   * @param session the MongoDB session.
   * @return Whether the update was successful or not.
   * @since 1.1.2
   */
  public static CompletionStage<Boolean> insertJson(
      final JsonObject json,
      final String environment,
      final MongoDatabase database,
      final ClientSession session) {
    return JsonClient.insert(database.getCollection(collection(json, environment)), json, session);
  }

  /**
   * Reconstructs the latest version of an aggregate using its event log.
   *
   * @param id the identifier of the aggregate.
   * @param type the aggregate type.
   * @param environment the environment in which this is run, e.g. "dev", "prd", etc.
   * @param database the database which contains the event log.
   * @return The reconstructed aggregate instance.
   * @since 1.1
   */
  public static CompletionStage<JsonObject> reconstruct(
      final String id, final String type, final String environment, final MongoDatabase database) {
    return reduce(
        events(id, type, environment, database), JsonUtil::emptyObject, Event::applyEvent);
  }

  /**
   * Reconstructs the latest version of an aggregate using its event log, starting from the given
   * snapshot.
   *
   * @param snapshot the snapshot of the aggregate instance to start from.
   * @param environment the environment in which this is run, e.g. "dev", "prd", etc.
   * @param database the database which contains the event log.
   * @return The reconstructed aggregate instance.
   * @since 1.1.2
   */
  public static CompletionStage<JsonObject> reconstruct(
      final JsonObject snapshot, final String environment, final MongoDatabase database) {
    return reduce(events(snapshot, environment, database), () -> snapshot, Event::applyEvent);
  }

  /**
   * Applies the published events one after the other and publishes the intermediate aggregate
   * instances.
   *
   * @param events the event publisher.
   * @return The aggregate instance publisher.
   * @since 1.2
   */
  public static Publisher<JsonObject> reconstructionPublisher(final Publisher<JsonObject> events) {
    return reconstructionPublisher(events, null);
  }

  /**
   * Applies the published events one after the other and publishes the intermediate aggregate
   * instances, starting from <code>snapshot</code>.
   *
   * @param events the event publisher.
   * @param snapshot the snapshot from where reconstruction begins.
   * @return The aggregate instance publisher.
   * @since 1.2
   */
  public static Publisher<JsonObject> reconstructionPublisher(
      final Publisher<JsonObject> events, final JsonObject snapshot) {
    return subscribe(events, new Mapper<>(applyEvent(snapshot)));
  }

  /**
   * Restores the latest version of an aggregate in the aggregate snapshot collection using its
   * event log.
   *
   * @param id the identifier of the aggregate.
   * @param type the aggregate type.
   * @param environment the environment in which this is run, e.g. "dev", "prd", etc.
   * @param database the database which contains the event log.
   * @return The reconstructed aggregate instance.
   * @since 1.1
   */
  public static CompletionStage<JsonObject> restore(
      final String id, final String type, final String environment, final MongoDatabase database) {
    return reconstruct(id, type, environment, database)
        .thenComposeAsync(json -> restoreReconstructed(json, environment, database));
  }

  /**
   * This resolves all "href" fields in subobjects that contain only a "href" field. The object
   * referred to by a href is fetched and its fields are added to the subject. Those fields must not
   * be changed by a reducer.
   *
   * @param json the given JSON object.
   * @param environment the environment in which the aggregate lives, e.g. test, acceptance,
   *     production, etc. This is added as a suffix to the collection name.
   * @param database the MongoDB database.
   * @return The resolved JSON object.
   * @since 1.1.3
   */
  public static CompletionStage<JsonObject> resolve(
      final JsonObject json, final String environment, final MongoDatabase database) {
    return resolve(list(json), environment, database).thenApply(list -> list.get(0));
  }

  /**
   * This resolves all "href" fields in subobjects that contain only a "href" field. The object
   * referred to by a href is fetched and its fields are added to the subject. Those fields must not
   * be changed by a reducer.
   *
   * @param json the given JSON objects. Common "href" fields will be fetched only once.
   * @param environment the environment in which the aggregate lives, e.g. test, acceptance,
   *     production, etc. This is added as a suffix to the collection name.
   * @param database the MongoDB database.
   * @return The resolved aggregate.
   * @since 1.1.3
   */
  public static CompletionStage<List<JsonObject>> resolve(
      final List<JsonObject> json, final String environment, final MongoDatabase database) {
    return Optional.of(hrefs(json.stream()))
        .filter(hrefs -> !hrefs.isEmpty())
        .map(
            hrefs ->
                fetchHrefs(hrefs, environment, database)
                    .thenApply(map -> json.stream().map(j -> resolve(j, map)).collect(toList())))
        .orElseGet(() -> completedFuture(json));
  }

  private static JsonObject resolve(
      final JsonObject json, final Map<Href, JsonObject> fetchedHrefs) {
    return transform(
        json,
        new Transformer(
            entry -> isObject(entry.value) && hrefOnly(entry.value.asJsonObject()),
            entry -> Optional.of(resolve(entry, fetchedHrefs))));
  }

  private static JsonEntry resolve(
      final JsonEntry entry, final Map<Href, JsonObject> fetchedHrefs) {
    return new JsonEntry(
        entry.path,
        ofNullable(fetchedHrefs.get(new Href(entry.value.asJsonObject().getString(HREF))))
            .map(
                fetched ->
                    add(
                            createObjectBuilder(entry.value.asJsonObject()).add(RESOLVED, true),
                            fetched)
                        .build())
            .orElse(entry.value.asJsonObject()));
  }

  /**
   * Restores the latest version of an aggregate in the aggregate snapshot collection using its
   * event log. If the snapshot is already the latest version then nothing is updated.
   *
   * @param snapshot the snapshot of th aggregate instance to start from.
   * @param environment the environment in which this is run, e.g. "dev", "prd", etc.
   * @param database the database which contains the event log.
   * @return The reconstructed aggregate instance.
   * @since 1.1.2
   */
  public static CompletionStage<JsonObject> restore(
      final JsonObject snapshot, final String environment, final MongoDatabase database) {
    return reconstruct(snapshot, environment, database)
        .thenComposeAsync(
            json ->
                json.getInt(SEQ) > snapshot.getInt(SEQ)
                    ? restoreReconstructed(json, environment, database)
                    : completedFuture(snapshot));
  }

  private static CompletionStage<JsonObject> restoreReconstructed(
      final JsonObject json, final String environment, final MongoDatabase database) {
    return isManagedObject(json)
        ? update(json, environment, database)
            .thenApply(result -> must(result, r -> r))
            .thenApply(result -> json)
        : completedFuture(emptyObject());
  }

  private static String suffix(final String environment) {
    return environment != null ? ("-" + environment) : "";
  }

  private static JsonObject technicalUpdateOperator(final JsonObject event) {
    return createObjectBuilder()
        .add(
            SET,
            createObjectBuilder()
                .add(SEQ, event.getInt(SEQ))
                .add(CORR, event.getString(CORR))
                .add(TIMESTAMP, event.getJsonNumber(TIMESTAMP)))
        .build();
  }

  /**
   * This removes the additions made by the <code>resolve</code> method.
   *
   * @param aggregate the given aggregate instance.
   * @return The unresolved aggregate instance.
   * @since 1.1.3
   */
  public static JsonObject unresolve(final JsonObject aggregate) {
    return transform(
        aggregate,
        new Transformer(
            entry -> isObject(entry.value) && entry.value.asJsonObject().containsKey(RESOLVED),
            entry ->
                Optional.of(
                    new JsonEntry(
                        entry.path,
                        copy(
                                entry.value.asJsonObject(),
                                createObjectBuilder(),
                                key -> key.equals(HREF))
                            .build()))));
  }

  /**
   * Updates <code>json</code> in the collection of which the name is <code>
   * &lt;aggregate-type&gt;-(event-|command-|-)&lt;environment&gt;</code>. The identifier in the
   * collection is the identifier of the JSON Event Sourcing object. If the object doesn't exist yet
   * it is inserted.
   *
   * @param json the given JSON Event Sourcing object, which can be an aggregate, event or a
   *     command.
   * @param environment the environment in which the aggregate lives, e.g. test, acceptance,
   *     production, etc. This is added as a suffix to the collection name.
   * @param database the MongoDB database.
   * @return Whether the update was successful or not.
   * @since 1.0
   */
  public static CompletionStage<Boolean> update(
      final JsonObject json, final String environment, final MongoDatabase database) {
    return update(json, environment, database, null);
  }

  /**
   * Updates the collection with the same name as the aggregate type. The identifier in the
   * collection is the identifier of the JSON Event Sourcing object. If the object doesn't exist yet
   * it is inserted.
   *
   * @param json the given JSON Event Sourcing object, which can be an aggregate, event or a
   *     command.
   * @param environment the environment in which the aggregate lives, e.g. test, acceptance,
   *     production, etc. This is added as a suffix to the collection name.
   * @param database the MongoDB database.
   * @param session the MongoDB session.
   * @return Whether the update was successful or not.
   * @since 1.1.2
   */
  public static CompletionStage<Boolean> update(
      final JsonObject json,
      final String environment,
      final MongoDatabase database,
      final ClientSession session) {
    return JsonClient.update(
        database.getCollection(collection(json, environment)), json, json.getString(ID), session);
  }

  /**
   * Updates the <code>collection</code> with <code>json</code>. If the object doesn't exist yet it
   * is inserted.
   *
   * @param json the given JSON Event Sourcing object, which can be an aggregate, event or a
   *     command.
   * @param id the identifier for the object.
   * @param collection the name of the collection.
   * @param database the MongoDB database.
   * @return Whether the update was successful or not.
   * @since 1.0
   */
  public static CompletionStage<Boolean> update(
      final JsonObject json,
      final String id,
      final String collection,
      final MongoDatabase database) {
    return JsonClient.update(database.getCollection(collection), json, id);
  }

  /**
   * Updates the <code>collection</code> with <code>json</code>. If the object doesn't exist yet it
   * is inserted.
   *
   * @param json the given JSON Event Sourcing object, which can be an aggregate, event or a
   *     command.
   * @param id the identifier for the object.
   * @param collection the name of the collection.
   * @param database the MongoDB database.
   * @param session the MongoDB session.
   * @return Whether the update was successful or not.
   * @since 1.1.2
   */
  public static CompletionStage<Boolean> update(
      final JsonObject json,
      final String id,
      final String collection,
      final MongoDatabase database,
      final ClientSession session) {
    return JsonClient.update(database.getCollection(collection), json, id, session);
  }

  /**
   * This updates an aggregate instance with the smallest number of changes using the operations in
   * the event.
   *
   * @param collection the aggregate collection.
   * @param currentState the current state of the aggregate instance that is about to be changed.
   * @param event the event with the operations.
   * @return Whether the update was successful or not.
   * @since 1.3.11
   */
  public static CompletionStage<Boolean> updateAggregate(
      final MongoCollection<Document> collection,
      final JsonObject currentState,
      final JsonObject event) {
    final Bson filter = eq(ID, currentState.getString(ID));

    return exec(
            collection,
            c ->
                c.bulkWrite(
                    concat(
                            Stream.of(technicalUpdateOperator(event)),
                            updateOperators(
                                currentState,
                                event.getJsonArray(OPS).stream()
                                    .filter(JsonUtil::isObject)
                                    .map(JsonValue::asJsonObject)))
                        .map(op -> new UpdateOneModel<Document>(filter, fromJson(op)))
                        .collect(toList()),
                    new BulkWriteOptions().ordered(true)))
        .thenApply(BulkWriteResult::wasAcknowledged)
        .exceptionally(
            e -> {
              deleteOne(collection, filter);
              rethrow(e);
              return false;
            });
  }

  /**
   * Wraps the <code>reducer</code> with a function that resolves the current state and unresolves
   * the result.
   *
   * @param reducer the given reducer.
   * @param environment the environment in which the aggregate lives, e.g. test, acceptance,
   *     production, etc. This is added as a suffix to the collection name.
   * @param database the MongoDB database.
   * @return The new aggregate instance.
   * @since 1.1.3
   */
  public static Reducer withResolver(
      final Reducer reducer, final String environment, final MongoDatabase database) {
    return (command, state) ->
        resolve(list(command, state), environment, database)
            .thenComposeAsync(resolved -> reducer.apply(resolved.get(0), resolved.get(1)))
            .thenApply(Mongo::unresolve);
  }
}
