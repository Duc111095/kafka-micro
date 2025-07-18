package com.ducnh.payment.domain;

public class Customer {

	private Long id;
	private String name;
	private int amountAvailable;
	private int amountReserved;
	
	public Customer() {
		
	}
	
	public Customer(Long id, String name, int amountAvailable, int amountReserved) {
		this.id = id;
		this.name = name;
		this.amountAvailable = amountAvailable;
		this.amountReserved = amountReserved;
	}
	
	public Long getId() {
		return id;
	}
	
	public void setId(Long id) {
		this.id = id;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public int getAmountAvailable() {
		return amountAvailable;
	}
	
	public void setAmountAvailable(int amount) {
		this.amountAvailable = amount;
	}
	
	public int getAmountReserved() {
		return amountReserved;
	}
	
	public void setAmountReserved(int amountReserved) {
		this.amountReserved = amountReserved;
	}
	
	@Override
	public String toString() {
		return "Customer{" + 
				"id=" + id +
				", name='" + name + "'" + 
				", amountAvailable=" + amountAvailable + 
				", amountReserved=" + amountReserved + 
				"}";
	}
}
