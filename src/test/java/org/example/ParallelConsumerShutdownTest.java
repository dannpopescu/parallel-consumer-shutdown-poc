package org.example;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class ParallelConsumerShutdownTest {

    static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.0"));

    private ParallelEoSStreamProcessor<String, String> pc;
    private String topic;
    private Producer<String, String> producer;

    static {
        kafkaContainer.start();
    }

    @BeforeEach
    void setUp() {
        ParallelConsumerOptions<String, String> pcOpts = ParallelConsumerOptions.<String, String>builder()
                .consumer(kafkaConsumer())
                .ordering(PARTITION)
                .build();
        pc = new ParallelEoSStreamProcessor<>(pcOpts);

        topic = "TestTopic-" + UUID.randomUUID();
        pc.subscribe(List.of(topic));

        producer = kafkaProducer();
    }

    @Test
    void givenRecordInFlightAndEmptyQueue_whenCloseDrainFirst_thenFinishInFlightThenClose() throws InterruptedException {
        var recordsStartedToProcess = new AtomicInteger();
        var processingBarrier = new CountDownLatch(1);
        var recordsFinishedToProcess = new AtomicInteger();

        produceMessages(1);

        pc.poll(ctx -> {
            recordsStartedToProcess.incrementAndGet();
            awaitOnLatch(processingBarrier);
            recordsFinishedToProcess.incrementAndGet();
        });

        await().untilAtomic(recordsStartedToProcess, is(equalTo(1)));

        new Thread(() -> pc.closeDrainFirst(Duration.ofSeconds(30))).start();
        Thread.sleep(2000);

        processingBarrier.countDown();

        await().until(() -> pc.isClosedOrFailed() || recordsFinishedToProcess.get() == 1);

        assertEquals(1, recordsFinishedToProcess.get(), "User consumption function was interrupted");
    }

    @Test
    void givenRecordsInFlightAndMoreWaitingInQueue_whenCloseDrainFirst_thenFinishInFlightAndQueuedThenCloseWithoutPollingMoreMessages() throws InterruptedException {
        var recordsToProduce = 2; // 1 in flight + 1 waiting in queue
        var recordsToProduceAfterClose = 10;

        var recordsStartedToProcess = new AtomicInteger();
        var recordsFinishedToProcess = new AtomicInteger();

        var processingBarrier = new CountDownLatch(1);

        produceMessages(recordsToProduce);

        pc.poll(ctx -> {
            recordsStartedToProcess.incrementAndGet();
            awaitOnLatch(processingBarrier);
            recordsFinishedToProcess.incrementAndGet();
        });

        await().untilAtomic(recordsStartedToProcess, is(equalTo(1)));

        new Thread(() -> pc.closeDrainFirst(Duration.ofSeconds(30))).start();
        Thread.sleep(2000);

        produceMessages(recordsToProduceAfterClose);

        processingBarrier.countDown();

        await().until(() -> pc.isClosedOrFailed() || recordsStartedToProcess.get() == recordsToProduce + recordsToProduceAfterClose);

        assertEquals(recordsToProduce, recordsStartedToProcess.get());
        assertEquals(recordsToProduce, recordsFinishedToProcess.get());
    }

    private void awaitOnLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void produceMessages(int numberOfMessages) {
        IntStream.range(0, numberOfMessages)
                .mapToObj(i -> producer.send(new ProducerRecord<>(topic, "key" + i, "value")))
                .toList()
                .forEach(recordMetadataFuture -> {
                    try {
                        recordMetadataFuture.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static Consumer<String, String> kafkaConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "TEST_GROUP_ID");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    private static Producer<String, String> kafkaProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(props);
    }
}