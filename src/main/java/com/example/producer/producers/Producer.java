package com.example.producer.producers;

import com.example.producer.model.FoodOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class Producer {

    @Value("${topic.name}")
    private String orderTopic;

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public Producer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Productor: responsable de recibir el pedido de alimentos y publicarlo como un mensaje para Kafka.
     *
     * En el siguiente metoddo convertimos el objeto FoodOrder en una cadena en formato JSON, para que pueda recibirse como una cadena en el microservicio del consumidor.
     *
     * Y enviamos el mensaje, pasando el tema en el que publicar (referido en el String orderTopic como la variable de entorno) y el pedido como un mensaje.
     *
     * @param foodOrder
     * @return
     * @throws JsonProcessingException
     */
    public String sendMessage(FoodOrder foodOrder) throws JsonProcessingException {
        String orderAsMessage = objectMapper.writeValueAsString(foodOrder);
        kafkaTemplate.send(orderTopic, orderAsMessage);

        log.info("food order produced {}", orderAsMessage);

        return "message sent";
    }
}