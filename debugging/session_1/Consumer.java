// Kafka Streams consumer: event-time windows over `inventory`.
// A window is emitted only when stream time (the latest event timestamp seen)
// passes the window close, i.e. when a later event arrives. No wall-clock flushes.
//
// On start it asks for the window seconds and the window type (1 = tumbling, 2 = session).
// To skip the prompts, set env vars:
//   WINDOW_SECONDS tumbling: window size | session: inactivity gap
//   WINDOW_TYPE    tumbling | session
//
// Run:  ./setup.sh   (once)   then   java --enable-native-access=ALL-UNNAMED -cp 'libs/*' Consumer.java
// e.g.  WINDOW_TYPE=session WINDOW_SECONDS=10 java --enable-native-access=ALL-UNNAMED -cp 'libs/*' Consumer.java

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

public class Consumer {
    static final String TOPIC = "inventory";
    static final String BASE_APP_ID = "inventory-kstream-demo";
    static final String BOOTSTRAP = "localhost:9092";
    static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'IST'").withZone(IST);
    static final DateTimeFormatter HMS = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(IST);
    static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        Scanner in = new Scanner(System.in);
        String envSeconds = System.getenv("WINDOW_SECONDS");
        String envType = System.getenv("WINDOW_TYPE");
        Duration window = Duration.ofSeconds(envSeconds != null ? Long.parseLong(envSeconds) : askSeconds(in));
        boolean session = envType != null ? parseType(envType) : askSession(in);
        // Separate app id per window type: their state stores are incompatible.
        String appId = session ? BASE_APP_ID + "-session" : BASE_APP_ID;

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        // No record cache: every update reaches suppress() right away, so it sees stream time immediately.
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        // First start reads only new events, not the old ones already in the topic.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        StreamsBuilder builder = new StreamsBuilder();
        KGroupedStream<String, String> grouped = builder
                .stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .peek(Consumer::received)
                .groupByKey();

        // Collect every raw JSON event of the window, one per line.
        Initializer<String> init = () -> "";
        Aggregator<String, String, String> collect = (key, event, all) -> join(all, event);

        KTable<Windowed<String>, String> windows = session
                // Session: starts at the first event, extends while events arrive < gap apart,
                // closes once stream time passes (last event + gap). Merger joins sessions bridged by an event.
                ? grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(window))
                        .aggregate(init, collect, (key, a, b) -> join(a, b),
                                Materialized.with(Serdes.String(), Serdes.String()))
                // Tumbling: fixed epoch-aligned slots [k*size, (k+1)*size).
                : grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(window))
                        .aggregate(init, collect, Materialized.with(Serdes.String(), Serdes.String()));

        // Hold results until stream time passes the window close, then emit once.
        windows.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .foreach(Consumer::process);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();

        System.out.printf("""

                ===== inventory kstream consumer started =====
                  started_at      : %s
                  broker          : %s
                  application id  : %s
                  topic           : %s
                  window          : %s
                  emits when      : %s
                  timezone        : %s
                ==============================================
                %n""", FMT.format(Instant.now()), BOOTSTRAP, appId, TOPIC,
                session ? "session, inactivity gap " + window.toSeconds() + "s, no grace (event time)"
                        : "tumbling " + window.toSeconds() + "s, no grace (event time)",
                session ? "an event with time > last event + gap arrives"
                        : "an event with time >= window end arrives",
                IST);
    }

    static long askSeconds(Scanner in) {
        while (true) {
            System.out.print("Window in seconds (tumbling: window size, session: inactivity gap) [300]: ");
            String line = in.hasNextLine() ? in.nextLine().trim() : "";
            if (line.isEmpty()) return 300;
            try {
                long seconds = Long.parseLong(line);
                if (seconds > 0) return seconds;
            } catch (NumberFormatException ignored) {
            }
            System.out.println("  Please enter a positive whole number.");
        }
    }

    static boolean askSession(Scanner in) {
        System.out.println("Window type:");
        System.out.println("  1) Tumbling");
        System.out.println("  2) Session");
        while (true) {
            System.out.print("Select 1 or 2 [1]: ");
            String line = in.hasNextLine() ? in.nextLine().trim() : "";
            switch (line) {
                case "", "1": return false;
                case "2": return true;
                default: System.out.println("  Please enter 1 or 2.");
            }
        }
    }

    static boolean parseType(String type) {
        return switch (type.toLowerCase()) {
            case "tumbling" -> false;
            case "session" -> true;
            default -> throw new IllegalArgumentException("WINDOW_TYPE must be tumbling or session, got: " + type);
        };
    }

    static void received(String key, String json) {
        JsonNode e = parse(json);
        System.out.printf("  <- received event_id=%s updated_at=%s%n",
                e.path("event_id").asText("null"), ist(e.path("updated_at").asText()));
    }

    /** Called once per closed window with all events in it. */
    static void process(Windowed<String> windowKey, String all) {
        // Sorted by updated_at: merged sessions may concatenate out of order.
        String[] events = all.split("\n");
        Arrays.sort(events, (a, b) -> OffsetDateTime.parse(parse(a).path("updated_at").asText())
                .compareTo(OffsetDateTime.parse(parse(b).path("updated_at").asText())));
        System.out.printf("%n[%s] window %s - %s IST | key=%s | %d events%n",
                FMT.format(Instant.now()),
                HMS.format(windowKey.window().startTime()),
                HMS.format(windowKey.window().endTime()),
                windowKey.key(), events.length);
        for (String json : events) {
            JsonNode e = parse(json);
            System.out.printf("  event_id=%s product_id=%s remaining_inventory=%s updated_at=%s%n",
                    e.path("event_id").asText("null"),
                    e.path("product_id").asText(),
                    e.path("remaining_inventory").asText(),
                    ist(e.path("updated_at").asText()));
        }
    }

    static String join(String a, String b) {
        return a.isEmpty() ? b : b.isEmpty() ? a : a + "\n" + b;
    }

    static String ist(String isoTimestamp) {
        return FMT.format(OffsetDateTime.parse(isoTimestamp));
    }

    static JsonNode parse(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Bad event: " + json, ex);
        }
    }
}
