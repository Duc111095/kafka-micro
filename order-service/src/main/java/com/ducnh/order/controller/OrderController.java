package com.ducnh.order.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ducnh.base.domain.Order;
import com.ducnh.order.service.OrderGeneratorService;

import io.swagger.v3.oas.annotations.parameters.RequestBody;

@RestController
@RequestMapping("/orders")
public class OrderController {
	
	private static final Logger LOG = LoggerFactory.getLogger(OrderController.class);
	private AtomicLong id = new AtomicLong();
	private KafkaTemplate<Long, Order> template;
	private StreamsBuilderFactoryBean kafkaStreamsFactory;
	private OrderGeneratorService orderGeneratorService;
	
	public OrderController(KafkaTemplate<Long, Order> template, 
							StreamsBuilderFactoryBean kafkaStreamsFactory,
							OrderGeneratorService orderGeneratorService) {
		this.template = template;
		this.kafkaStreamsFactory = kafkaStreamsFactory;
		this.orderGeneratorService = orderGeneratorService;
	}
	
	@PostMapping
	public Order create(@RequestBody Order order) {
		order.setId(id.incrementAndGet());
		template.send("orders", order.getId(), order);
		LOG.info("Sent: {}", order);
		return order;
	}
	
	@PostMapping("/generate")
	public boolean create() {
		orderGeneratorService.generate();
		return true;
	} 
	
	@GetMapping
	public List<Order> all() {
		List<Order> orders = new ArrayList<>();
		ReadOnlyKeyValueStore<Long, Order> store = kafkaStreamsFactory
				.getKafkaStreams()
				.store(StoreQueryParameters.fromNameAndType(
						"orders", 
						QueryableStoreTypes.keyValueStore()));
		KeyValueIterator<Long, Order> it = store.all();
		it.forEachRemaining(kv -> orders.add(kv.value));
		return orders;
	}
}
