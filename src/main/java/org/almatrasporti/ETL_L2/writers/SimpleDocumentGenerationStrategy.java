package org.almatrasporti.ETL_L2.writers;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

public class SimpleDocumentGenerationStrategy implements IDocumentGenerationStrategy {
    public Document process(Document obj, MongoCollection collection) {
        obj.remove("VinVehicle");
        obj.remove("Timestamp");

        return obj;
    }
}
