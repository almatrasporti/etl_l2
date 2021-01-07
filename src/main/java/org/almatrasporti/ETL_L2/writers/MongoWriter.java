package org.almatrasporti.ETL_L2.writers;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.almatrasporti.common.utils.Config;
import org.bson.Document;

public class MongoWriter implements IWriter {

    private final String collectionName;
    private IDocumentGenerationStrategy documentGenerationStrategy;
    protected MongoClient mongoClient;
    protected MongoDatabase db;
    protected MongoCollection collection;

    public MongoWriter(String collectionName, IDocumentGenerationStrategy documentGenerationStrategy) {
        this.collectionName = collectionName;
        this.documentGenerationStrategy = documentGenerationStrategy;

        String connectionString = Config.getInstance().get("MongoDB.server");
        this.mongoClient = MongoClients.create(connectionString);
        this.db = mongoClient.getDatabase(Config.getInstance().get("MongoDB.db.name"));

        try {
            this.db.createCollection(this.collectionName);
        } catch (Exception e) {
        }

        this.collection = this.db.getCollection(this.collectionName);
    }

    public boolean upsertRecord(String recordValue) {
        Document obj = Document.parse(recordValue);
        Document key = new Document();
        key.append("VinVehicle", obj.getString("VinVehicle"))
                .append("Timestamp", obj.getInteger("Timestamp"));

        UpdateOptions options = new UpdateOptions().upsert(true);

        obj = this.documentGenerationStrategy.process(obj, this.collection);

        Document update = new Document();
        update.append("$set", obj);

        System.out.println(key);

        UpdateResult result = this.collection.updateOne(key, update, options);
        return result.wasAcknowledged();
    }
}
