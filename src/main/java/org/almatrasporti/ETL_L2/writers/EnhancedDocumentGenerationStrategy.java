package org.almatrasporti.ETL_L2.writers;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

public class EnhancedDocumentGenerationStrategy implements IDocumentGenerationStrategy {
    private MongoCollection collection;

    public Document process(Document currentDocument, MongoCollection collection) {
        this.collection = collection;
        Document previousDocument = this.getLastRecord((String)currentDocument.get("VinVehicle"));

        currentDocument.append("DeltaOdometer", this.getDelta("Odometer", previousDocument, currentDocument));
        currentDocument.append("DeltaLifeConsumption", this.getDelta("LifeConsumption", previousDocument, currentDocument));

        currentDocument.remove("VinVehicle");
        currentDocument.remove("Timestamp");

        return currentDocument;
    }

    private Document getLastRecord(String vinVehicle) {
        Document query = new Document();
        query.append("VinVehicle", vinVehicle);

        return (Document) this.collection.find(query).sort((new Document()).append("Timestamp", -1)).first();

    }

    private Integer getDelta(String field, Document previousDocument, Document currentDocument) {
        Integer currentValue = (Integer)currentDocument.get(field);

        if (previousDocument == null) {
            return currentValue;
        }

        return currentValue - (Integer)previousDocument.get(field);
    }
}
