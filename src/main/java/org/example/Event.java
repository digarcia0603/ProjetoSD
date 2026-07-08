package org.example;

import javax.xml.crypto.Data;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Event {
    private String product;
    private int quantity;
    private double price;

    public Event(String product, int quantity, double price) {
        this.product = product;
        this.quantity = quantity;
        this.price = price;
    }

    public String getProduct() {
        return product;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getPrice() {
        return price;
    }

    // Serialização (Transformar Objeto -> Bytes)
    public void write(DataOutputStream out) throws IOException {
        out.writeUTF(product);
        out.writeInt(quantity);
        out.writeDouble(price);
    }

    // Deserialização (Tranformar Bytes -> Objeto)
    public static Event read(DataInputStream in) throws IOException {
        String prod = in.readUTF();
        int qty = in.readInt();
        double price = in.readDouble();
        return new Event(prod, qty, price);
    }

    @Override
    public String toString() {
        return "Product: " + product + " | Qty: " + quantity + " | Price: " + price;
    }
}