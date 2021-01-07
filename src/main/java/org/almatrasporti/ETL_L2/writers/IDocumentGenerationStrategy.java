package org.almatrasporti.ETL_L2.writers;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

public interface IDocumentGenerationStrategy {
    public Document process(Document obj, MongoCollection collection);
}
