package net.pincette.jes.util;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.type;
import static com.mongodb.client.model.Sorts.ascending;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Logger.getGlobal;
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
import static net.pincette.json.JsonUtil.toJsonPointer;
import static net.pincette.json.Transform.transform;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.Collection.countDocuments;
import static net.pincette.mongo.Collection.deleteOne;
import static net.pincette.mongo.Collection.exec;
import static net.pincette.mongo.Collection.insertOne;
import static net.pincette.mongo.JsonClient.findPublisher;
import static net.pincette.mongo.Patch.updateOperators;
import static net.pincette.mongo.Session.inTransaction;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Reducer.reduce;
import static net.pincette.rs.Util.join;
import static net.pincette.rs.Util.subscribe;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.composeAsyncStream;
import static net.pincette.util.Util.must;
import static org.bson.BsonType.STRING;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.json.Transform.JsonEntry;
import net.pincette.json.Transform.Transformer;
import net.pincette.mongo.JsonClient;
import net.pincette.rs.Mapper;
import net.pincette.util.State;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonString;
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
  private static final String FIELD_ID = "id";
  private static final String EVENT_ID = ID + "." + FIELD_ID;
  private static final String FIELD_SEQ = "seq";
  private static final String EVENT_SEQ = ID + "." + FIELD_SEQ;
  private static final String HREF = "href";
  private static final Bson OLD_EVENTS = and(exists(SEQ), type(ID, STRING));
  private static final String RESOLVED = "_resolved";
  private static final String SET = "$set";
  private static final Bson SORT_EVENTS = sort(ascending(EVENT_SEQ));

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

  private static long countOldEvents(final MongoCollection<BsonDocument> collection) {
    return countDocuments(collection, OLD_EVENTS).toCompletableFuture().join();
  }

  private static String eventCollection(final String type, final String environment) {
    return type + "-event" + suffix(environment);
  }

  /**
   * Converts an event to a representation for MongoDB, where the <code>_id</code> field is a BSON
   * document with the fields <code>id</code> and <code>seq</code>, in that order.
   *
   * @param event the given event.
   * @return The converted event.
   * @since 2.0
   */
  public static BsonDocument eventToMongo(final JsonObject event) {
    return new BsonDocument(
        concat(
                Stream.of(new BsonElement(ID, mongoEventKey(event))),
                event.entrySet().stream()
                    .filter(e -> !e.getKey().equals(ID) && !e.getKey().equals(SEQ))
                    .map(e -> new BsonElement(e.getKey(), fromJson(e.getValue()))))
            .collect(toList()));
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
    return mongoToEvents(
        JsonClient.aggregationPublisher(
            database.getCollection(eventCollection(type, environment)),
            list(match(eq(EVENT_ID, id)), SORT_EVENTS)));
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
    return mongoToEvents(
        JsonClient.aggregationPublisher(
            database.getCollection(eventCollection(snapshot.getString(TYPE), environment)),
            list(
                match(
                    and(
                        eq(EVENT_ID, snapshot.getString(ID)),
                        gt(EVENT_SEQ, snapshot.getInt(SEQ, -1)))),
                SORT_EVENTS)));
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

  private static JsonObject fixId(final JsonObject event) {
    return createObjectBuilder(event).add(ID, stripSequence(event.getString(ID))).build();
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

  private static BsonDocument mongoEventKey(final JsonObject event) {
    return new BsonDocument(
        list(
            new BsonElement(FIELD_ID, new BsonString(event.getString(ID))),
            new BsonElement(FIELD_SEQ, new BsonInt32(event.getInt(SEQ)))));
  }

  /**
   * Converts a representation of an event for MongoDB, where the <code>_id</code> field is an
   * object with the fields <code>id</code> and <code>seq</code>, to an event.
   *
   * @param event the given mongoDB representation of the event.
   * @return The converted event.
   * @since 2.0
   */
  public static JsonObject mongoToEvent(final JsonObject event) {
    return createObjectBuilder(event)
        .remove(ID)
        .add(ID, event.getValue(toJsonPointer(EVENT_ID)))
        .add(SEQ, event.getValue(toJsonPointer(EVENT_SEQ)))
        .build();
  }

  private static Publisher<JsonObject> mongoToEvents(final Publisher<JsonObject> mongoEvents) {
    return with(mongoEvents).map(Mongo::mongoToEvent).get();
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

  private static CompletionStage<Boolean> replaceEvent(
      final MongoCollection<BsonDocument> collection,
      final JsonObject event,
      final ClientSession session) {
    return deleteOne(collection, session, eq(ID, event.getString(ID)))
        .thenApply(DeleteResult::wasAcknowledged)
        .thenApply(result -> must(result, r -> r))
        .thenComposeAsync(result -> insertOne(collection, session, eventToMongo(fixId(event))))
        .thenApply(InsertOneResult::wasAcknowledged)
        .thenApply(result -> must(result, r -> r));
  }

  /**
   * Restores the latest version of an aggregate in the aggregate snapshot collection using its
   * event log.
   *
   * @param id the identifier of the aggregate.
   * @param type the aggregate type.
   * @param environment the environment in which this is run, e.g. "dev", "prd", etc.
   * @param database the database which contains the event log.
   * @param session the client session this is run in.
   * @return The reconstructed aggregate instance.
   * @since 1.1
   */
  public static CompletionStage<JsonObject> restore(
      final String id,
      final String type,
      final String environment,
      final MongoDatabase database,
      final ClientSession session) {
    return reconstruct(id, type, environment, database)
        .thenComposeAsync(json -> restoreReconstructed(json, environment, database, session));
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
   * @param session the client session in which this is run.
   * @return The reconstructed aggregate instance.
   * @since 1.1.2
   */
  public static CompletionStage<JsonObject> restore(
      final JsonObject snapshot,
      final String environment,
      final MongoDatabase database,
      final ClientSession session) {
    return reconstruct(snapshot, environment, database)
        .thenComposeAsync(
            json ->
                json.getInt(SEQ) > snapshot.getInt(SEQ)
                    ? restoreReconstructed(json, environment, database, session)
                    : completedFuture(snapshot));
  }

  private static CompletionStage<JsonObject> restoreReconstructed(
      final JsonObject json,
      final String environment,
      final MongoDatabase database,
      final ClientSession session) {
    return isManagedObject(json)
        ? update(json, environment, database, session)
            .thenApply(result -> must(result, r -> r))
            .thenApply(result -> json)
        : completedFuture(emptyObject());
  }

  private static String stripSequence(final String id) {
    return Optional.of(id.lastIndexOf('-'))
        .filter(index -> index != -1)
        .map(index -> id.substring(0, index))
        .filter(net.pincette.util.Util::isUUID)
        .orElse(id);
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
   * @param session the client session this runs in.
   * @return Whether the update was successful or not.
   * @since 1.3.11
   */
  public static CompletionStage<Boolean> updateAggregate(
      final MongoCollection<Document> collection,
      final JsonObject currentState,
      final JsonObject event,
      final ClientSession session) {
    final Bson filter = eq(ID, currentState.getString(ID));

    return exec(
            collection,
            c ->
                c.bulkWrite(
                    session,
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
        .thenApply(result -> must(result, r -> r));
  }

  /**
   * Upgrades events to version 2.0. This function is idempotent. It will block until it is done.
   *
   * @param type the aggregate type.
   * @param environment the environment in which the aggregate lives, e.g. test, acceptance,
   *     production, etc. This is added as a suffix to the collection name.
   * @param database the MongoDB database.
   * @param session the MongoDB client session, which is used for transactions.
   * @param logger the logger to which progress is written. It may be <code>null</code>.
   * @since 2.0
   */
  public static void upgradeEventLog(
      final String type,
      final String environment,
      final MongoDatabase database,
      final ClientSession session,
      final Logger logger) {
    final MongoCollection<BsonDocument> collection =
        database.getCollection(eventCollection(type, environment), BsonDocument.class);
    final State<Long> count = new State<>(0L);

    if (logger != null) {
      logger.info(
          () -> "Upgrading " + countOldEvents(collection) + " events for type " + type + ".");
    }

    join(
        with(findPublisher(database.getCollection(eventCollection(type, environment)), OLD_EVENTS))
            .map(
                event -> {
                  if (logger != null && count.set(count.get() + 1) % 10000 == 0) {
                    logger.info(() -> "Processed " + count.get() + " events.");
                  }
                  return event;
                })
            .mapAsync(
                event ->
                    inTransaction(s -> replaceEvent(collection, event, s), session)
                        .exceptionally(
                            e -> {
                              getGlobal().log(SEVERE, e.getMessage(), e);
                              return false;
                            }))
            .get());

    if (logger != null) {
      logger.info(() -> "Done upgrading events for type " + type + ".");
    }
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
