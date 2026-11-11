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
