package io.github.victorwingert.stripnul;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class StripNulTest {

    private static final Schema ROW = SchemaBuilder.struct().name("row").optional()
            .field("ID", Schema.INT32_SCHEMA)
            .field("CHAVE", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private static final Schema ENVELOPE = SchemaBuilder.struct().name("envelope")
            .field("before", ROW)
            .field("after", ROW)
            .field("op", Schema.STRING_SCHEMA)
            .build();

    private static Struct row(int id, String chave) {
        return new Struct(ROW).put("ID", id).put("CHAVE", chave);
    }

    private static SinkRecord update(Struct before, Struct after) {
        Struct value = new Struct(ENVELOPE).put("before", before).put("after", after).put("op", "u");
        return new SinkRecord("topic", 0, Schema.INT32_SCHEMA, 1, ENVELOPE, value, 0);
    }

    private static Struct value(SinkRecord record) {
        return (Struct) record.value();
    }

    // The bug this exists for: a NUL inside the after of a Debezium event
    // reaches PostgreSQL and stops the sink (GLO_ANEXO.ANX_ST_CHAVEPK).
    @Test
    void stripsANulInsideTheAfterOfAnEnvelope() {
        SinkRecord out = new StripNul.Value<SinkRecord>().apply(update(row(1, "a\u0000b"), row(1, "11716781\u0000")));

        assertEquals("ab", value(out).getStruct("before").getString("CHAVE"));
        assertEquals("11716781", value(out).getStruct("after").getString("CHAVE"));
        assertEquals("u", value(out).getString("op"));
        assertEquals(1, value(out).getStruct("after").getInt32("ID"));
    }

    @Test
    void returnsTheSameRecordWhenThereIsNoNul() {
        SinkRecord in = update(null, row(1, "53;1;39;G;140;1;9969"));

        assertSame(in, new StripNul.Value<SinkRecord>().apply(in));
    }

    // A delete is a null value; the sink needs it to remove the row.
    @Test
    void passesATombstoneThrough() {
        SinkRecord in = new SinkRecord("topic", 0, Schema.INT32_SCHEMA, 1, null, null, 0);

        assertNull(new StripNul.Value<SinkRecord>().apply(in).value());
    }

    @Test
    void stripsANulInTheKey() {
        Schema key = SchemaBuilder.struct().field("COD", Schema.STRING_SCHEMA).build();
        SinkRecord in = new SinkRecord("topic", 0, key, new Struct(key).put("COD", "\u0000X"), null, null, 0);

        Struct out = (Struct) new StripNul.Key<SinkRecord>().apply(in).key();

        assertEquals("X", out.getString("COD"));
    }
}
