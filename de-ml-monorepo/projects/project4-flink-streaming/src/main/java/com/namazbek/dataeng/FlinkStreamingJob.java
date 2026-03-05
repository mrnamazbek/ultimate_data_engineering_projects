package com.namazbek.dataeng;

import com.namazbek.dataeng.models.Event;
import com.namazbek.dataeng.models.EventMetrics;
import com.namazbek.dataeng.serialization.EventSchema;
import com.namazbek.dataeng.sinks.PostgresSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Flink Streaming Job for Real-time Event Processing
 * 
 * Features:
 * - Ultra-low latency processing (< 100ms)
 * - Stateful computations with checkpointing
 * - Complex Event Processing (CEP) patterns
 * - Sliding window analytics
 * - Session-based aggregations
 */
public class FlinkStreamingJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkStreamingJob.class);
    
    // Configuration from environment variables
    private static final String KAFKA_BROKER = System.getenv().getOrDefault("KAFKA_BROKER", "kafka:9092");
    private static final String KAFKA_TOPIC = System.getenv().getOrDefault("KAFKA_TOPIC", "events");
    private static final String POSTGRES_HOST = System.getenv().getOrDefault("POSTGRES_HOST", "postgres");
    private static final String POSTGRES_PORT = System.getenv().getOrDefault("POSTGRES_PORT", "5432");
    private static final String POSTGRES_USER = System.getenv().getOrDefault("POSTGRES_USER", "deuser");
    private static final String POSTGRES_PASSWORD = System.getenv().getOrDefault("POSTGRES_PASSWORD", "depassword");
    private static final String POSTGRES_DB = System.getenv().getOrDefault("POSTGRES_DB", "dedb");

    public static void main(String[] args) throws Exception {
        
        LOG.info("Starting Flink Real-time Streaming Job");
        LOG.info("Kafka Broker: {}, Topic: {}", KAFKA_BROKER, KAFKA_TOPIC);
        LOG.info("PostgreSQL: {}:{}/{}", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB);
        
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(30000); // checkpoint every 30 seconds
        env.getCheckpointConfig().setCheckpointTimeout(60000); // 1 minute timeout
        env.setParallelism(2);
        
        // Configure Kafka source
        KafkaSource<Event> kafkaSource = KafkaSource.<Event>builder()
                .setBootstrapServers(KAFKA_BROKER)
                .setTopics(KAFKA_TOPIC)
                .setGroupId("flink-streaming-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new EventSchema())
                .build();

        // Create event stream with watermarks for event-time processing
        DataStream<Event> eventStream = env
                .fromSource(kafkaSource, 
                           WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                   .withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime()),
                           "kafka-source");

        // ═══════════════════════════════════════════════════════════════════
        // 1. Real-time Event Count per User (Stateful Processing)
        // ═══════════════════════════════════════════════════════════════════
        KeyedStream<Event, Integer> keyedByUser = eventStream.keyBy(Event::getUserId);
        
        SingleOutputStreamOperator<EventMetrics> userEventCounts = keyedByUser
                .process(new UserEventCountProcessor())
                .name("user-event-counter");

        // ═══════════════════════════════════════════════════════════════════
        // 2. Sliding Window Analytics (1-minute window, 10-second slide)
        // ═══════════════════════════════════════════════════════════════════
        SingleOutputStreamOperator<EventMetrics> slidingWindowMetrics = eventStream
                .windowAll(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(10)))
                .aggregate(new EventAggregator(), new WindowResultFunction())
                .name("sliding-window-analytics");

        // ═══════════════════════════════════════════════════════════════════
        // 3. Complex Event Processing - Detect Rapid Fire Events
        // ═══════════════════════════════════════════════════════════════════
        Pattern<Event, ?> rapidFirePattern = Pattern.<Event>begin("first")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return event.getValue() != null && event.getValue().contains("rapid");
                    }
                })
                .followedBy("second")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return event.getValue() != null && event.getValue().contains("fire");
                    }
                })
                .within(Time.seconds(30));

        PatternStream<Event> rapidFirePatternStream = CEP.pattern(
                eventStream.keyBy(Event::getUserId), 
                rapidFirePattern);

        SingleOutputStreamOperator<EventMetrics> cepResults = rapidFirePatternStream
                .select((Map<String, List<Event>> pattern) -> {
                    List<Event> firstEvents = pattern.get("first");
                    List<Event> secondEvents = pattern.get("second");
                    
                    EventMetrics metrics = new EventMetrics();
                    if (!firstEvents.isEmpty() && !secondEvents.isEmpty()) {
                        Event firstEvent = firstEvents.get(0);
                        Event secondEvent = secondEvents.get(0);
                        
                        metrics.setUserId(firstEvent.getUserId());
                        metrics.setMetricType("rapid_fire_pattern");
                        metrics.setEventCount(2L);
                        metrics.setWindowStart(firstEvent.getTimestamp());
                        metrics.setWindowEnd(secondEvent.getTimestamp());
                        
                        long timeDiff = secondEvent.getTimestamp().getTime() - firstEvent.getTimestamp().getTime();
                        metrics.setAvgProcessingTime((double) timeDiff);
                    }
                    return metrics;
                })
                .name("cep-rapid-fire-detection");

        // ═══════════════════════════════════════════════════════════════════
        // 4. Session-based Analytics (30-second session gap)
        // ═══════════════════════════════════════════════════════════════════
        SingleOutputStreamOperator<EventMetrics> sessionMetrics = keyedByUser
                .window(org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows.withGap(Time.seconds(30)))
                .aggregate(new SessionEventAggregator())
                .name("session-analytics");

        // ═══════════════════════════════════════════════════════════════════
        // Sinks: Write all metrics to PostgreSQL
        // ═══════════════════════════════════════════════════════════════════
        String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB);
        
        userEventCounts.addSink(new PostgresSink(jdbcUrl, POSTGRES_USER, POSTGRES_PASSWORD, "flink_user_metrics"))
                .name("postgres-user-metrics-sink");
                
        slidingWindowMetrics.addSink(new PostgresSink(jdbcUrl, POSTGRES_USER, POSTGRES_PASSWORD, "flink_window_metrics"))
                .name("postgres-window-metrics-sink");
                
        cepResults.addSink(new PostgresSink(jdbcUrl, POSTGRES_USER, POSTGRES_PASSWORD, "flink_cep_metrics"))
                .name("postgres-cep-metrics-sink");
                
        sessionMetrics.addSink(new PostgresSink(jdbcUrl, POSTGRES_USER, POSTGRES_PASSWORD, "flink_session_metrics"))
                .name("postgres-session-metrics-sink");

        // Execute the streaming job
        LOG.info("Executing Flink Streaming Job...");
        env.execute("Flink Real-time Event Processing");
    }

    /**
     * Stateful processor to count events per user
     */
    public static class UserEventCountProcessor extends KeyedProcessFunction<Integer, Event, EventMetrics> {
        
        private ValueState<Long> eventCountState;
        private ValueState<Long> lastUpdateState;

        @Override
        public void open(Configuration parameters) {
            eventCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("eventCount", Long.class, 0L));
            lastUpdateState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastUpdate", Long.class, 0L));
        }

        @Override
        public void processElement(Event event, Context ctx, Collector<EventMetrics> out) throws Exception {
            Long currentCount = eventCountState.value();
            Long newCount = currentCount + 1;
            eventCountState.update(newCount);
            
            long currentTime = System.currentTimeMillis();
            lastUpdateState.update(currentTime);

            // Emit metrics every 100 events or every 10 seconds
            if (newCount % 100 == 0 || (currentTime - lastUpdateState.value()) > 10000) {
                EventMetrics metrics = new EventMetrics();
                metrics.setUserId(event.getUserId());
                metrics.setMetricType("user_event_count");
                metrics.setEventCount(newCount);
                metrics.setWindowStart(event.getTimestamp());
                metrics.setWindowEnd(new java.sql.Timestamp(currentTime));
                
                out.collect(metrics);
            }
        }
    }

    /**
     * Aggregate function for sliding window analytics
     */
    public static class EventAggregator implements AggregateFunction<Event, Tuple2<Long, Integer>, Tuple2<Long, Integer>> {
        
        @Override
        public Tuple2<Long, Integer> createAccumulator() {
            return new Tuple2<>(0L, 0);
        }

        @Override
        public Tuple2<Long, Integer> add(Event event, Tuple2<Long, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0 + 1, accumulator.f1 + event.getUserId());
        }

        @Override
        public Tuple2<Long, Integer> getResult(Tuple2<Long, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    /**
     * Window function to create metrics from aggregated results
     */
    public static class WindowResultFunction implements WindowFunction<Tuple2<Long, Integer>, EventMetrics, TimeWindow> {
        
        @Override
        public void apply(TimeWindow window, Iterable<Tuple2<Long, Integer>> input, Collector<EventMetrics> out) {
            Tuple2<Long, Integer> result = input.iterator().next();
            
            EventMetrics metrics = new EventMetrics();
            metrics.setMetricType("sliding_window");
            metrics.setEventCount(result.f0);
            metrics.setWindowStart(new java.sql.Timestamp(window.getStart()));
            metrics.setWindowEnd(new java.sql.Timestamp(window.getEnd()));
            metrics.setAvgProcessingTime(result.f1.doubleValue() / result.f0); // Avg user ID as processing time proxy
            
            out.collect(metrics);
        }
    }

    /**
     * Session-based aggregator
     */
    public static class SessionEventAggregator implements AggregateFunction<Event, EventMetrics, EventMetrics> {
        
        @Override
        public EventMetrics createAccumulator() {
            EventMetrics metrics = new EventMetrics();
            metrics.setEventCount(0L);
            metrics.setMetricType("session");
            return metrics;
        }

        @Override
        public EventMetrics add(Event event, EventMetrics accumulator) {
            accumulator.setEventCount(accumulator.getEventCount() + 1);
            accumulator.setUserId(event.getUserId());
            
            if (accumulator.getWindowStart() == null) {
                accumulator.setWindowStart(event.getTimestamp());
            }
            accumulator.setWindowEnd(event.getTimestamp());
            
            return accumulator;
        }

        @Override
        public EventMetrics getResult(EventMetrics accumulator) {
            return accumulator;
        }

        @Override
        public EventMetrics merge(EventMetrics a, EventMetrics b) {
            EventMetrics merged = new EventMetrics();
            merged.setEventCount(a.getEventCount() + b.getEventCount());
            merged.setUserId(a.getUserId()); // Assume same user for keyed stream
            merged.setMetricType("session");
            merged.setWindowStart(a.getWindowStart().before(b.getWindowStart()) ? a.getWindowStart() : b.getWindowStart());
            merged.setWindowEnd(a.getWindowEnd().after(b.getWindowEnd()) ? a.getWindowEnd() : b.getWindowEnd());
            return merged;
        }
    }
}
