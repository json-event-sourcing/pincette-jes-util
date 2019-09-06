package net.pincette.jes.util;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Filters.or;
import static net.pincette.jes.util.JsonFields.DELETED;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.BsonUtil.toDocument;
import static net.pincette.mongo.Collection.replaceOne;
import static net.pincette.util.Collections.list;

import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.util.concurrent.CompletionStage;
import javax.json.JsonObject;
import org.bson.conversions.Bson;

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
