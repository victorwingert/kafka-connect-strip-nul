# kafka-connect-strip-nul

A Kafka Connect transform that removes the NUL character (U+0000) from every string in the record
key or value, at any depth of nested structs — including the `before` and `after` of a Debezium
change event.

PostgreSQL does not accept NUL in text and fails the write with
`invalid byte sequence for encoding "UTF8": 0x00`. With `errors.tolerance=none`, one such value stops
the JDBC sink for every table it writes.

A record without NUL is returned unchanged.

## Use

```yaml
transforms: stripnul
transforms.stripnul.type: io.github.victorwingert.stripnul.StripNul$Value
```

`StripNul$Key` does the same to the key. Strings and structs are visited; arrays and maps are not.

## DropUnavailable

Oracle logs a LOB only when a statement sets it. An update that changes another column therefore
carries `__debezium_unavailable_value` in place of the CLOB, with `lob.enabled` true or false, and
the Debezium JDBC sink writes that text over the real value (debezium/dbz#850, debezium/dbz#1137).
`DropUnavailable$Value` removes every field of the `after` that holds the placeholder, from the
`before` and the `after` and from their schema. The sink builds the upsert from the fields of the
record, so the column keeps the value it had.

```yaml
transforms: stripnulvalue,dropunavailable
transforms.dropunavailable.type: io.github.victorwingert.stripnul.DropUnavailable$Value
# optional: the unavailable.value.placeholder of the source
transforms.dropunavailable.placeholder: __debezium_unavailable_value
```

An update that sets the CLOB keeps it: the new value is in the `after`. A delete, a tombstone and a
record without the placeholder pass through unchanged.

## Install

Each release has the jar and its SHA-512. In a Strimzi `KafkaConnect` build:

```yaml
- name: strip-nul
  artifacts:
    - type: jar
      url: https://github.com/victorwingert/kafka-connect-strip-nul/releases/download/vX.Y.Z/kafka-connect-strip-nul-X.Y.Z.jar
      sha512sum: <the .sha512 asset>
```

## Build

```sh
mvn verify
```
