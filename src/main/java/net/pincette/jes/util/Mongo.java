package net.pincette.jes.util;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Sorts.ascending;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static javax.json.Json.createObjectBuilder;
import static javax.json.Json.createPatch;
import static net.pincette.jes.util.Command.isCommand;
import static net.pincette.jes.util.Event.isEvent;
import static net.pincette.jes.util.JsonFields.CORR;
import static net.pincette.jes.util.JsonFields.DELETED;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.JWT;
import static net.pincette.jes.util.JsonFields.OPS;
import static net.pincette.jes.util.JsonFields.SEQ;
import static net.pincette.jes.util.JsonFields.TIMESTAMP;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.jes.util.Util.isManagedObject;
import static net.pincette.json.JsonUtil.add;
import static net.pincette.json.JsonUtil.copy;
import static net.pincette.json.JsonUtil.emptyObject;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.Transform.transform;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.BsonUtil.toDocument;
import static net.pincette.mongo.Collection.insertOne;
import static net.pincette.mongo.Collection.replaceOne;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Reducer.reduce;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.composeAsyncStream;
import static net.pincette.util.Util.must;

import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import javax.json.JsonObject;
import net.pincette.json.JsonUtil;
import net.pincette.json.Transform.JsonEntry;
import net.pincette.json.Transform.Transformer;
import net.pincette.mongo.BsonUtil;
import net.pincette.mongo.Collection;
import org.bson.BsonDocument;
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
  public static final Bson NOT_DELETED = or(list(not(exists(DELETED)), eq(DELETED, false)));
  private static final String HREF = "href";
  private static final String RESOLVED = "_resolved";

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

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param pipeline the given pipeline.
   * @return The list of objects.
   * @since 1.0.4
   */
  public static CompletionStage<List<JsonObject>> aggregate(
      final MongoCollection<Document> collection, final List<? extends Bson> pipeline) {
    return aggregate(collection, pipeline, null);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param pipeline the given pipeline.
   * @return The list of objects.
   * @since 1.1.2
   */
  public static CompletionStage<List<JsonObject>> aggregate(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final List<? extends Bson> pipeline) {
    return aggregate(collection, session, pipeline, null);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param pipeline the given pipeline.
   * @param setParameters a function to set the parameters for the result set.
   * @return The list of objects.
   * @since 1.0.4
   */
  public static CompletionStage<List<JsonObject>> aggregate(
      final MongoCollection<Document> collection,
      final List<? extends Bson> pipeline,
      final UnaryOperator<AggregatePublisher<BsonDocument>> setParameters) {
    return Collection.aggregate(collection, pipeline, BsonDocument.class, setParameters)
        .thenApply(Mongo::toJson);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param pipeline the given pipeline.
   * @param setParameters a function to set the parameters for the result set.
   * @return The list of objects.
   * @since 1.1.2
   */
  public static CompletionStage<List<JsonObject>> aggregate(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final List<? extends Bson> pipeline,
      final UnaryOperator<AggregatePublisher<BsonDocument>> setParameters) {
    return Collection.aggregate(collection, session, pipeline, BsonDocument.class, setParameters)
        .thenApply(Mongo::toJson);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param pipeline the given pipeline.
   * @return The object publisher.
   * @since 1.1
   */
  public static Publisher<JsonObject> aggregationPublisher(
      final MongoCollection<Document> collection, final List<? extends Bson> pipeline) {
    return aggregationPublisher(collection, pipeline, null);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param pipeline the given pipeline.
   * @param setParameters a function to set the parameters for the result set.
   * @return The object publisher.
   * @since 1.1
   */
  public static Publisher<JsonObject> aggregationPublisher(
      final MongoCollection<Document> collection,
      final List<? extends Bson> pipeline,
      final UnaryOperator<AggregatePublisher<BsonDocument>> setParameters) {
    return aggregationPublisher(
        () -> collection.aggregate(pipeline, BsonDocument.class), setParameters);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param pipeline the given pipeline.
   * @param setParameters a function to set the parameters for the result set.
   * @return The object publisher.
   * @since 1.1.2
   */
  public static Publisher<JsonObject> aggregationPublisher(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final List<? extends Bson> pipeline,
      final UnaryOperator<AggregatePublisher<BsonDocument>> setParameters) {
    return aggregationPublisher(
        () -> collection.aggregate(session, pipeline, BsonDocument.class), setParameters);
  }

  private static Publisher<JsonObject> aggregationPublisher(
      final Supplier<AggregatePublisher<BsonDocument>> operation,
      final UnaryOperator<AggregatePublisher<BsonDocument>> setParameters) {
    return Optional.of(operation.get())
        .map(a -> setParameters != null ? setParameters.apply(a) : a)
        .map(Mongo::toJson)
        .orElseGet(net.pincette.rs.Util::empty);
  }

  private static JsonObject applyEvent(final JsonObject json, final JsonObject event) {
    return create(
            () ->
                createObjectBuilder(
                    createPatch(event.getJsonArray(OPS)).apply(json).asJsonObject()))
        .update(b -> b.add(ID, stripSequenceNumber(event.getString(ID))))
        .update(b -> b.add(TYPE, event.getString(TYPE)))
        .update(b -> b.add(CORR, event.getString(CORR)))
        .update(b -> b.add(SEQ, event.getInt(SEQ)))
        .update(b -> b.add(TIMESTAMP, event.getJsonNumber(TIMESTAMP)))
        .updateIf(() -> Optional.ofNullable(event.getJsonObject(JWT)), (b, jwt) -> b.add(JWT, jwt))
        .build()
        .build();
  }

  private static String collection(final JsonObject json, final String environment) {
    return json.getString(TYPE) + collectionInfix(json) + environment;
  }

  private static String collection(final Href href, final String environment) {
    return href.type + "-" + environment;
  }

  private static String collectionInfix(final JsonObject json) {
    final Supplier<String> tryCommand = () -> isCommand(json) ? "-command-" : "-";

    return isEvent(json) ? "-event-" : tryCommand.get();
  }

  private static String eventCollection(final String type, final String environment) {
    return type + "-event-" + environment;
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
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param filter the given filter.
   * @return The list of objects.
   * @since 1.0.2
   */
  public static CompletionStage<List<JsonObject>> find(
      final MongoCollection<Document> collection, final Bson filter) {
    return find(collection, filter, null);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param filter the given filter.
   * @return The list of objects.
   * @since 1.1.2
   */
  public static CompletionStage<List<JsonObject>> find(
      final MongoCollection<Document> collection, final ClientSession session, final Bson filter) {
    return find(collection, session, filter, null);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param filter the given filter.
   * @param setParameters a function to set the parameters for the result set.
   * @return The list of objects.
   * @since 1.0.2
   */
  public static CompletionStage<List<JsonObject>> find(
      final MongoCollection<Document> collection,
      final Bson filter,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return Collection.find(collection, filter, BsonDocument.class, setParameters)
        .thenApply(Mongo::toJson);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param filter the given filter.
   * @param setParameters a function to set the parameters for the result set.
   * @return The list of objects.
   * @since 1.0.2
   */
  public static CompletionStage<List<JsonObject>> find(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final Bson filter,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return Collection.find(collection, session, filter, BsonDocument.class, setParameters)
        .thenApply(Mongo::toJson);
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
    return findOne(database.getCollection(collection(href, environment)), eq(ID, href.id));
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param filter the given filter.
   * @return The object publisher.
   * @since 1.1
   */
  public static Publisher<JsonObject> findPublisher(
      final MongoCollection<Document> collection, final Bson filter) {
    return findPublisher(collection, filter, null);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param filter the given filter.
   * @param setParameters a function to set the parameters for the result set.
   * @return The object publisher.
   * @since 1.1
   */
  public static Publisher<JsonObject> findPublisher(
      final MongoCollection<Document> collection,
      final Bson filter,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return findPublisher(() -> collection.find(filter, BsonDocument.class), setParameters);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param filter the given filter.
   * @param setParameters a function to set the parameters for the result set.
   * @return The object publisher.
   * @since 1.1.2
   */
  public static Publisher<JsonObject> findPublisher(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final Bson filter,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return findPublisher(() -> collection.find(session, filter, BsonDocument.class), setParameters);
  }

  private static Publisher<JsonObject> findPublisher(
      final Supplier<FindPublisher<BsonDocument>> operation,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return Optional.of(operation.get())
        .map(a -> setParameters != null ? setParameters.apply(a) : a)
        .map(Mongo::toJson)
        .orElseGet(net.pincette.rs.Util::empty);
  }

  /**
   * Finds a JSON object. Only one should match the <code>filter</code>, otherwise the result will
   * be empty.
   *
   * @param collection the MongoDB collection.
   * @param filter the given filter.
   * @return The optional result.
   * @since 1.0.2
   */
  public static CompletionStage<Optional<JsonObject>> findOne(
      final MongoCollection<Document> collection, final Bson filter) {
    return Collection.findOne(collection, filter, BsonDocument.class, null)
        .thenApply(result -> result.map(BsonUtil::fromBson));
  }

  /**
   * Finds a JSON object. Only one should match the <code>filter</code>, otherwise the result will
   * be empty.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param filter the given filter.
   * @return The optional result.
   * @since 1.1.2
   */
  public static CompletionStage<Optional<JsonObject>> findOne(
      final MongoCollection<Document> collection, final ClientSession session, final Bson filter) {
    return Collection.findOne(collection, session, filter, BsonDocument.class, null)
        .thenApply(result -> result.map(BsonUtil::fromBson));
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
    return insert(json, collection, database, null);
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
    final Document document = toDocument(fromJson(json));
    final MongoCollection<Document> mongoCollection = database.getCollection(collection);

    return (session != null
            ? insertOne(mongoCollection, session, document)
            : insertOne(mongoCollection, document))
        .thenApply(success -> true);
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
    return insert(json, environment, database, null);
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
    return insert(json, collection(json, environment), database, session);
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
        aggregationPublisher(
            database.getCollection(eventCollection(type, environment)),
            list(match(regex(ID, "^" + id + ".*")), sort(ascending(ID)))),
        JsonUtil::emptyObject,
        Mongo::applyEvent);
  }

  /**
   * Reconstructs the latest version of an aggregate using its event log, starting from the given
   * snapshot.
   *
   * @param snapshot the snapshot of th aggregate instance to start from.
   * @param environment the environment in which this is run, e.g. "dev", "prd", etc.
   * @param database the database which contains the event log.
   * @return The reconstructed aggregate instance.
   * @since 1.1.2
   */
  public static CompletionStage<JsonObject> reconstruct(
      final JsonObject snapshot, final String environment, final MongoDatabase database) {
    return reduce(
        aggregationPublisher(
            database.getCollection(eventCollection(snapshot.getString(TYPE), environment)),
            list(
                match(
                    and(
                        regex(ID, "^" + snapshot.getString(ID) + ".*"),
                        gt(SEQ, snapshot.getInt(SEQ, -1)))),
                sort(ascending(ID)))),
        () -> snapshot,
        Mongo::applyEvent);
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
            entry ->
                Optional.of(
                    new JsonEntry(
                        entry.path,
                        add(
                                createObjectBuilder(entry.value.asJsonObject()).add(RESOLVED, true),
                                fetchedHrefs.get(
                                    new Href(entry.value.asJsonObject().getString(HREF))))
                            .build()))));
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

  private static String stripSequenceNumber(final String id) {
    return Optional.of(id.lastIndexOf('-'))
        .filter(index -> index != -1)
        .map(index -> id.substring(0, index))
        .orElse(id);
  }

  private static List<JsonObject> toJson(final List<BsonDocument> list) {
    return list.stream().map(BsonUtil::fromBson).collect(toList());
  }

  private static Publisher<JsonObject> toJson(final Publisher<BsonDocument> pub) {
    return with(pub).map(BsonUtil::fromBson).get();
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
    return update(json, json.getString(ID), collection(json, environment), database, session);
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
    return update(json, id, collection, database, null);
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
    final Document document = toDocument(fromJson(json));
    final Bson filter = eq(ID, id);
    final MongoCollection<Document> mongoCollection = database.getCollection(collection);
    final ReplaceOptions options = new ReplaceOptions().upsert(true);

    return (session != null
            ? replaceOne(mongoCollection, session, filter, document, options)
            : replaceOne(mongoCollection, filter, document, options))
        .thenApply(UpdateResult::wasAcknowledged);
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
