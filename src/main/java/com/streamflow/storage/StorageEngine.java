package com.streamflow.storage;

import com.streamflow.core.Message;
import com.streamflow.core.TopicPartition;

import java.util.List;

public interface StorageEngine {
    void initialize();
    void shutdown();
    
    long append(TopicPartition partition, Message message);
    List<Message> read(TopicPartition partition, long startOffset, int maxMessages);
    
    long getLogEndOffset(TopicPartition partition);
    long getLogStartOffset(TopicPartition partition);
    
    void createPartition(TopicPartition partition);
    void deletePartition(TopicPartition partition);
    
    void flush(TopicPartition partition);
    void truncate(TopicPartition partition, long offset);
    
    StorageMetrics getMetrics(TopicPartition partition);
}