package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.message.KafkaMessage;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KafkaConsumer {
    @KafkaListener(topics = "test_topic", groupId = "test-group")
    public void batchConsume(List<ConsumerRecord<String, KafkaMessage>> records, Acknowledgment ack) {
        // 遍历批量消息
        for (ConsumerRecord<String, KafkaMessage> record : records) {
            String key = record.key();
            KafkaMessage message = record.value();
            long offset = record.offset(); // 消息偏移量
            int partition = record.partition(); // 消息分区

            // 业务处理（如入库、调用接口）
            try {
                System.out.println("recieve message from:" + partition + ", offset:" + offset + ", key:" + key + ", content:" + message.getContent());
                // doBusiness(message);
            } catch (Exception e) {
                System.err.println("业务处理失败：" + e.getMessage());
                // 异常处理（重试/死信队列）
                continue;
            }
        }

        // 手动提交偏移量（所有消息处理完成后提交，保证不重复消费）
        ack.acknowledge();
    }
}
