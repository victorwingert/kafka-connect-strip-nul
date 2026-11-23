package io.github.victorwingert.stripnul;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class DropUnavailableTest {

    private static final String PLACEHOLDER = "__debezium_unavailable_value";

    private static final Schema ROW = SchemaBuilder.struct().name("mega.INTER.EST_ITENSPEDCOMPRA.Value").optional()
            .field("ITP_IN_SEQUENCIA", Schema.INT32_SCHEMA)
            .field("ITP_ST_SITUACAO", Schema.OPTIONAL_STRING_SCHEMA)
            .field("ITP_ST_COMPLEMENTO", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private static final Schema ENVELOPE = SchemaBuilder.struct().name("mega.INTER.EST_ITENSPEDCOMPRA.Envelope")
            .version(2)
            .field("before", ROW)
            .field("after", ROW)
            .field("op", Schema.STRING_SCHEMA)
            .build();

    private static Struct row(int seq, String situacao, String complemento) {
        return new Struct(ROW).put("ITP_IN_SEQUENCIA", seq).put("ITP_ST_SITUACAO", situacao)
                .put("ITP_ST_COMPLEMENTO", complemento);
    }

    private static SinkRecord event(String op, Struct before, Struct after) {
        Struct value = new Struct(ENVELOPE).put("before", before).put("after", after).put("op", op);
        return new SinkRecord("mega.INTER.EST_ITENSPEDCOMPRA", 0, Schema.INT32_SCHEMA, 1, ENVELOPE, value, 0);
    }

    private static Struct after(SinkRecord record) {
        return ((Struct) record.value()).getStruct("after");
    }

    private static List<String> names(Schema schema) {
        return schema.fields().stream().map(Field::name).toList();
    }

    private static SinkRecord apply(SinkRecord in) {
        return new DropUnavailable.Value<SinkRecord>().apply(in);
    }

    // The bug this exists for: an update of another column of the item
    // carries the placeholder in place of the CLOB, and the sink's upsert
    // would write it over the complement in the mega database.
    @Test
    void dropsTheClobThatCarriesThePlaceholderFromAnUpdate() {
        SinkRecord out = apply(event("u", row(1, "PA", PLACEHOLDER), row(1, "PB", PLACEHOLDER)));

        assertEquals(List.of("ITP_IN_SEQUENCIA", "ITP_ST_SITUACAO"), names(after(out).schema()));
        assertEquals("PB", after(out).getString("ITP_ST_SITUACAO"));
        assertEquals(List.of("ITP_IN_SEQUENCIA", "ITP_ST_SITUACAO"),
                names(((Struct) out.value()).getStruct("before").schema()));
        assertEquals("u", ((Struct) out.value()).getString("op"));
    }

    // The JDBC sink tells a Debezium event by the envelope schema name.
    @Test
    void keepsTheNamesAndVersionOfTheSchemas() {
        SinkRecord out = apply(event("u", null, row(1, "PB", PLACEHOLDER)));

        assertEquals("mega.INTER.EST_ITENSPEDCOMPRA.Envelope", out.valueSchema().name());
        assertEquals(2, out.valueSchema().version());
        assertEquals("mega.INTER.EST_ITENSPEDCOMPRA.Value", after(out).schema().name());
        assertEquals(true, after(out).schema().isOptional());
    }

    @Test
    void keepsAClobThatChanged() {
        SinkRecord in = event("u", row(1, "PA", PLACEHOLDER), row(1, "PA", "ENTREGAR NO PORTÃO 2"));

        assertSame(in, apply(in));
    }

    @Test
    void returnsTheSameRecordWithoutThePlaceholder() {
        SinkRecord in = event("u", row(1, "PA", "A"), row(1, "PA", "B"));

        assertSame(in, apply(in));
    }

    @Test
    void keepsAClobSetToNull() {
        SinkRecord in = event("u", row(1, "PA", "A"), row(1, "PA", null));

        assertSame(in, apply(in));
    }

    // The sink deletes by the key alone.
    @Test
    void passesADeleteThrough() {
        SinkRecord in = event("d", row(1, "PA", PLACEHOLDER), null);

        assertSame(in, apply(in));
    }

    @Test
    void passesATombstoneThrough() {
        SinkRecord in = new SinkRecord("topic", 0, Schema.INT32_SCHEMA, 1, null, null, 0);

        assertSame(in, apply(in));
    }

    @Test
    void reusesTheSchemaForTheSameDroppedFields() {
        SinkRecord first = apply(event("u", null, row(1, "PA", PLACEHOLDER)));
        DropUnavailable.Value<SinkRecord> transform = new DropUnavailable.Value<>();

        Schema a = transform.apply(event("u", null, row(1, "PA", PLACEHOLDER))).valueSchema();
        Schema b = transform.apply(event("u", null, row(2, "PB", PLACEHOLDER))).valueSchema();

        assertSame(a, b);
        assertEquals(first.valueSchema(), a);
    }

    @Test
    void readsThePlaceholderFromTheConfiguration() {
        DropUnavailable.Value<SinkRecord> transform = new DropUnavailable.Value<>();
        transform.configure(Map.of(DropUnavailable.PLACEHOLDER_CONFIG, "__nao_veio"));

        SinkRecord out = transform.apply(event("u", null, row(1, "PA", "__nao_veio")));

        assertNull(after(out).schema().field("ITP_ST_COMPLEMENTO"));
        assertSame(out.valueSchema().field("op").schema(), Schema.STRING_SCHEMA);
    }
}
