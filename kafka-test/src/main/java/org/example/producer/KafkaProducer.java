package org.example.producer;

import jakarta.annotation.Resource;
import org.example.message.KafkaMessage;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class KafkaProducer {
    @Resource
    private KafkaTemplate<String, KafkaMessage> kafkaTemplate;

    private static final String TOPIC_NAME = "test_topic";

    /**
     * 同步发送消息（阻塞等待结果）
     */
    public void sendSyncMessage(Long id, String content) {
        KafkaMessage message = new KafkaMessage();
        message.setId(id);
        message.setContent(content);
        message.setSendTime(LocalDateTime.now());

        try {
            // 同步发送：topic + key + 消息体（key用于分区路由，可选）
            SendResult<String, KafkaMessage> result = kafkaTemplate.send(TOPIC_NAME, id.toString(), message).get();
            System.out.println("同步发送成功：" + result.getRecordMetadata().offset());
        } catch (Exception e) {
            System.err.println("同步发送失败：" + e.getMessage());
            // 异常处理（重试/告警）
        }
    }
}
