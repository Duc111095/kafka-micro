package com.ducnh.order.service;

import org.springframework.stereotype.Service;

import com.ducnh.base.domain.Order;

@Service
public class OrderManageService {
	
	public Order confirm(Order orderPayment, Order orderStock) {
		Order o = new Order(orderPayment.getId(),
				orderPayment.getCustomerId(),
				orderPayment.getProductId(),
				orderPayment.getProductCount(),
				orderPayment.getPrice());
		if (orderPayment.getStatus().equals("ACCEPT") &&
				orderStock.getStatus().equals("ACCEPT")) {
			o.setStatus("CONFIRMED");
		} else if (orderPayment.getStatus().equals("REJECT") &&
				orderStock.getStatus().equals("REJECT")) {
			o.setStatus("REJECT");
		} else if (orderPayment.getStatus().equals("REJECT") || 
				orderStock.getStatus().equals("REJECT")) {
			String source = orderPayment.getStatus().equals("REJECT") ? "PAYMENT" : "STOCK";
			o.setStatus("ROLLBACK");
			o.setSource(source);
		}
		return o;	
	}
}
