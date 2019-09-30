package com.linuxacademy.ccdak.testing;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author will
 */
public class MyConsumerTest {
    
    MockConsumer<Integer, String> mockConsumer;
    MyConsumer myConsumer;
    
    // Contains data sent so System.out during the test.
    private ByteArrayOutputStream systemOutContent;
    private final PrintStream originalSystemOut = System.out;
    
    @Before
    public void setUp() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        myConsumer = new MyConsumer();
        myConsumer.consumer = mockConsumer;
    }
    
    @Before
    public void setUpStreams() {
        systemOutContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(systemOutContent));
    }

    @After
    public void restoreStreams() {
        System.setOut(originalSystemOut);
    }
    
    @Test
    public void testHandleRecords_output() {
        // Verify that the testHandleRecords writes the correct data to System.out
        String topic = "test_topic";
        ConsumerRecord<Integer, String> record = new ConsumerRecord<>(topic, 0, 1, 2, "Test value");
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new LinkedHashMap<>();
        records.put(new TopicPartition(topic, 0), Arrays.asList(record));
        ConsumerRecords<Integer, String> consumerRecords = new ConsumerRecords<>(records);
        
        myConsumer.handleRecords(consumerRecords);
        Assert.assertEquals("key=2, value=Test value, topic=test_topic, partition=0, offset=1\n", systemOutContent.toString());
    }
    
}
