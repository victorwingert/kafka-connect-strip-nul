package io.github.victorwingert.stripnul;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * Removes the NUL character (U+0000) from every string in the record key or
 * value, at any depth of nested structs. PostgreSQL rejects NUL in text with
 * "invalid byte sequence for encoding UTF8: 0x00", and a JDBC sink with
 * errors.tolerance=none then stops for every table it writes.
 *
 * <p>A record without NUL is returned as is, so the common case costs one
 * scan and no copy.
 *
 * <pre>
 * transforms: stripnul
 * transforms.stripnul.type: io.github.victorwingert.stripnul.StripNul$Value
 * </pre>
 */
public abstract class StripNul<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String NUL = "\u0000";

    @Override
    public R apply(R record) {
        Object original = operatingValue(record);
        Object stripped = strip(original);
        return stripped == original ? record : newRecord(record, stripped);
    }

    static Object strip(Object value) {
        if (value instanceof String s) {
            return s.contains(NUL) ? s.replace(NUL, "") : s;
        }
        if (value instanceof Struct struct) {
            return strip(struct);
        }
        return value;
    }

    private static Struct strip(Struct struct) {
        Struct copy = null;
        for (Field field : struct.schema().fields()) {
            Object original = struct.get(field);
            Object stripped = strip(original);
            if (stripped != original && copy == null) {
                copy = copyOf(struct);
            }
            if (copy != null) {
                copy.put(field, stripped);
            }
        }
        return copy == null ? struct : copy;
    }

    private static Struct copyOf(Struct struct) {
        Struct copy = new Struct(struct.schema());
        for (Field field : struct.schema().fields()) {
            copy.put(field, struct.get(field));
        }
        return copy;
    }

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Object value);

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void close() {
    }

    /** Strips the record key. */
    public static class Key<R extends ConnectRecord<R>> extends StripNul<R> {
        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Object key) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), key,
                    record.valueSchema(), record.value(), record.timestamp(), record.headers());
        }
    }

    /** Strips the record value, including the before and after of a Debezium envelope. */
    public static class Value<R extends ConnectRecord<R>> extends StripNul<R> {
        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Object value) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                    record.valueSchema(), value, record.timestamp(), record.headers());
        }
    }
}
