package be.flink.sql.join.sample.io;

public interface PulsarSerializable {

    byte[] getMessageKey();

}
