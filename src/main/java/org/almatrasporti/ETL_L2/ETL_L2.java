package org.almatrasporti.ETL_L2;

import org.almatrasporti.ETL_L2.consumers.ConsumeToDocumentStore;
import org.almatrasporti.ETL_L2.writers.EnhancedDocumentGenerationStrategy;
import org.almatrasporti.ETL_L2.writers.IWriter;
import org.almatrasporti.ETL_L2.writers.MongoWriter;
import org.almatrasporti.ETL_L2.writers.SimpleDocumentGenerationStrategy;

import java.util.ArrayList;
import java.util.List;

public class ETL_L2 {

    ConsumeToDocumentStore consumer;

    public ETL_L2() {
        MongoWriter simpleWriter = new MongoWriter("simple_collection", new SimpleDocumentGenerationStrategy());
        MongoWriter enhancedWriter = new MongoWriter("enhanced_collection", new EnhancedDocumentGenerationStrategy());
        List<IWriter> writers = new ArrayList<IWriter>();
        writers.add(simpleWriter);
        writers.add(enhancedWriter);
        consumer = new ConsumeToDocumentStore(writers);
    }

    private void execute() {
        consumer.consume();
    }

    public static void main(String args[]) {
        ETL_L2 worker = new ETL_L2();
        worker.execute();
    }
}
