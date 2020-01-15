package net.pincette.jes.util;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Sorts.ascending;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static javax.json.Json.createObjectBuilder;
import static javax.json.Json.createPatch;
import static net.pincette.jes.util.JsonFields.CORR;
import static net.pincette.jes.util.JsonFields.DELETED;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.JWT;
import static net.pincette.jes.util.JsonFields.OPS;
import static net.pincette.jes.util.JsonFields.SEQ;
import static net.pincette.jes.util.JsonFields.TIMESTAMP;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.jes.util.Util.isManagedObject;
import static net.pincette.json.JsonUtil.emptyObject;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.BsonUtil.toDocument;
import static net.pincette.mongo.Collection.replaceOne;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Reducer.reduce;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Util.must;

import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;
import javax.json.JsonObject;
import net.pincette.json.JsonUtil;
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
        .thenApply(list -> list.stream().map(BsonUtil::fromBson).collect(toList()));
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
    return Optional.of(collection.aggregate(pipeline, BsonDocument.class))
        .map(a -> setParameters != null ? setParameters.apply(a) : a)
        .map(a -> with(a).map(BsonUtil::fromBson).get())
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
        .thenApply(list -> list.stream().map(BsonUtil::fromBson).collect(toList()));
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
    return Optional.of(collection.find(filter, BsonDocument.class))
        .map(a -> setParameters != null ? setParameters.apply(a) : a)
        .map(a -> with(a).map(BsonUtil::fromBson).get())
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
            database.getCollection(type + "-event-" + environment),
            list(match(regex(ID, "^" + id + ".*")), sort(ascending(ID)))),
        JsonUtil::emptyObject,
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
        .thenComposeAsync(
            json ->
                isManagedObject(json)
                    ? update(json, environment, database)
                        .thenApply(result -> must(result, r -> r))
                        .thenApply(result -> json)
                    : completedFuture(emptyObject()));
  }

  private static String stripSequenceNumber(final String id) {
    return Optional.of(id.lastIndexOf('-'))
        .filter(index -> index != -1)
        .map(index -> id.substring(0, index))
        .orElse(id);
  }

  /**
   * Updates the collection with the same name as the aggregate type. The identifier in the
   * collection is the identifier of the aggregate. If the aggregate doesn't exist yet it is
   * inserted.
   *
   * @param aggregate the given aggregate.
   * @param environment the environment in which the aggregate lives, e.g. test, acceptance,
   *     production, etc. This is added as a suffix to the collection name.
   * @param database the MongoDB database.
   * @return Whether the update was successful or not.
   * @since 1.0
   */
  public static CompletionStage<Boolean> update(
      final JsonObject aggregate, final String environment, final MongoDatabase database) {
    return update(
        aggregate,
        aggregate.getString(ID),
        aggregate.getString(TYPE) + "-" + environment,
        database);
  }

  /**
   * Updates the <code>collection</code> with the <code>managedObject</code>. If the object doesn't
   * exist yet it is inserted.
   *
   * @param managedObject the given managed object, which can be an aggregate, event or a command.
   * @param id the identifier for the object.
   * @param collection the name of the collection. This may be <code>null</code>.
   * @param database the MongoDB database.
   * @return Whether the update was successful or not.
   * @since 1.0
   */
  public static CompletionStage<Boolean> update(
      final JsonObject managedObject,
      final String id,
      final String collection,
      final MongoDatabase database) {
    return replaceOne(
            database.getCollection(collection),
            eq(ID, id),
            toDocument(fromJson(managedObject)),
            new ReplaceOptions().upsert(true))
        .thenApply(UpdateResult::wasAcknowledged);
  }
}
