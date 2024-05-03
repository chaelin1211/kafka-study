package com.example.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.Message;

import java.time.Duration;
import java.util.Map;

@SpringBootApplication
public class KafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}

}

// TODO kafka plugin 다운 받기 (현재 IDE 버전이랑 맞는게 없다	....)
// https://docs.spring.io/spring-kafka/reference/kafka/transactions.html

// 수동 offset 처리 or offset rollback 등의 관리
// https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/ooo-commits.html

@Configuration
class RunnerConfiguration {

	@Bean
	// Springboot가 떴을 떄
	ApplicationListener<ApplicationReadyEvent> readyEventApplicationListener (KafkaTemplate<Object, Object> kafkaTemplate) {
		return event -> send(kafkaTemplate);
	}

	private void send(KafkaTemplate<Object, Object> kafkaTemplate) {
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		var pageView = new PageView("new Page", 1000, "chaelin", "kafka");

		kafkaTemplate.send(KafkaConfiguration.PV_TOPIC, pageView);
		System.out.println("Message sent!!");
	}
}

@Configuration
class KafkaConfiguration {

	public static final String PV_TOPIC = "pv_topic";

	// 토픽 수신
	// ctrl+alt+c -> 자동 리팩토링
	@KafkaListener(topics = PV_TOPIC, groupId = "pv_group_id")
	public void onNewPageView(Message<PageView> pageViewMessage, Acknowledgment acknowledgment) {
		System.out.println("----" + Thread.currentThread());
		System.out.println("new page view" + pageViewMessage.getPayload());
		pageViewMessage.getHeaders().forEach(((s, o) -> System.out.println(s+"="+o)));

		try {
			acknowledgment.acknowledge();
		} catch (Exception e) {
			// sleep time
			// 처리하지 않고 넘어감
			acknowledgment.nack(Duration.ofMillis(2000));
		}
	}

	// JSON으로 보내서 Message로 감싸서 받을 수도 있고 사용하고자 하는 객체로 받아도 매핑 됨
//	public void onNewPageView(PageView pageViewMessage) {
//		System.out.println("----" + Thread.currentThread());
//		System.out.println("new page view" + pageViewMessage);
//	}

	@Bean
	NewTopic newTopic() {
		return new NewTopic(PV_TOPIC, 1, (short) 1);
		// 토픽 선언: 이름, 파티션 수, 리플리케이션 수
	}

	@Bean
	JsonMessageConverter jsonMessageConverter() {
		return new JsonMessageConverter();
	}

	@Bean
	KafkaTemplate<Object, Object> kafkaTemplate(ProducerFactory<Object, Object> objectObjectProducerFactory) {
		return new KafkaTemplate<>(objectObjectProducerFactory, Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class));
		// Map.of -> 불변
	}
}

record PageView(String page, long duration, String userId, String source) {

}
