package org.almatrasporti.ETL_L2.writers;

public interface IWriter {
    public boolean upsertRecord(String recordValue);
}
