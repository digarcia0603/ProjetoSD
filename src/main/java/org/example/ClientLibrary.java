package org.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.io.*;
import java.net.Socket;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Map;

public class ClientLibrary implements AutoCloseable {
    private final TaggedConnection taggedConnection;
    private final Demultiplexer demultiplexer;
    private final ReentrantLock lock = new ReentrantLock();
    private int tag = 0; // Contador de tags para gerar IDs únicos.

    // Mapas para sincronizar pedidos e respostas.
    private final Map <Integer, Condition> conditionsMap = new HashMap<>(); // Tag -> Onde dormir.
    private final Map <Integer, byte[]> responsesMap = new HashMap<>(); // Tag -> Resposta recebida.

    public ClientLibrary(String host, int port) throws IOException {
        Socket socket = new Socket(host, port);
        this.taggedConnection = new TaggedConnection(socket);
        // Liga o Demultiplexer a esta Library.
        this.demultiplexer = new Demultiplexer(taggedConnection);

        // Diz ao Demultiplexer: "Quando receberes dados, entrega-os a mim (ClientLibrary)".
        this.demultiplexer.setClientLibrary(this);
        this.demultiplexer.start();
    }

    // O método mágico que envia e espera pela resposta.
    private byte[] sendReceive(short requestType, byte[] data) throws IOException, InterruptedException {
        int myTag = -1;
        lock.lock();
        try {
            myTag = this.tag++; // Gera nova tag. (ex: tag 5)
            Condition cond = lock.newCondition(); // Cria nova condição para esta thread dormir.
            conditionsMap.put(myTag, cond); // Regista a condição que tem a tag 5.

            // 4. Envia o pedido pela rede com a Tag 5.
            taggedConnection.send(myTag, requestType, data); // Envia pedido.

            // Loop de espera (enquanto a resposta da tag 5 não chegar ao mapa, dorme).
            while (!responsesMap.containsKey(myTag)) {
                cond.await(); // Dorme aqui e espera pelo sinal.
            }

            // 6. Acordou! Remove a resposta do mapa e devolve-a.
            return responsesMap.remove(myTag); // Retorna os dados e limpa o mapa.
        } finally {
            if (myTag != -1) conditionsMap.remove(myTag);
            lock.unlock();
        }
    }

    // Chamado pelo Demultiplexer quando chega uma resposta do socket.
    public void addResponse(int tag, byte[] data) {
        lock.lock();
        try {
            // Se houver alguém à espera desta tag...
            if (conditionsMap.containsKey(tag)) {
                responsesMap.put(tag, data); // Guarda os dados.
                conditionsMap.get(tag).signalAll(); // ACORDA a thread que estava no 'await'.
            }
        } finally {
            lock.unlock();
        }
    }


    // --- MÉTODOS DE NEGÓCIO (Wrappers) ---
    // Todos seguem o padrão: Serializar -> sendReceive -> Deserializar.

    public boolean register(String username, String password) throws IOException, InterruptedException {
        System.out.println("[ClientLib] Sending registration request for: " + username);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeUTF(username);
        out.writeUTF(password);

        System.out.println("DEBUG: REGISTER_REQUEST value is: " + RequestType.REGISTER_REQUEST.getValue());

        byte[] response = sendReceive(RequestType.REGISTER_REQUEST.getValue(), baos.toByteArray());

        // Lê a resposta (que sabemos ser um booleano).
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(response));
        return in.readBoolean();
    }

    public boolean authenticate(String username, String password) throws IOException, InterruptedException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeUTF(username);
        out.writeUTF(password);

        byte[] response = sendReceive(RequestType.AUTH_REQUEST.getValue(), baos.toByteArray());

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(response));
        return in.readBoolean();
    }

    // Exemplo de uso: Adicionar Evento.
    public void addEvent(String product, int quantity, double price) throws IOException, InterruptedException {
        // Serializa os argumentos para bytes.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeUTF(product);
        out.writeInt(quantity);
        out.writeDouble(price);

        // Envia e espera pelo ACK (mesmo que seja vazio).
        sendReceive(RequestType.ADD_EVENT.getValue(), baos.toByteArray());
    }

    public int getSoldQuantity(String product, int days) throws IOException, InterruptedException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeUTF(product);
        out.writeInt(days);

        byte[] response = sendReceive(RequestType.AGGREGATE_QTY.getValue(), baos.toByteArray());
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(response))) {
            return in.readInt();
        }
    }

    public double getSalesVolume(String product, int days) throws IOException, InterruptedException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeUTF(product);
        out.writeInt(days);

        byte[] response = sendReceive(RequestType.AGGREGATE_VOL.getValue(), baos.toByteArray());

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(response))) {
            return in.readDouble();
        }
    }

    public double getAveragePrice(String product, int days) throws IOException, InterruptedException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeUTF(product);
        out.writeInt(days);

        byte[] response = sendReceive(RequestType.AGGREGATE_AVG.getValue(), baos.toByteArray());

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(response))) {
            return in.readDouble();
        }
    }

    public double getMaxPrice(String product, int days) throws IOException, InterruptedException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeUTF(product);
        out.writeInt(days);

        byte[] response = sendReceive(RequestType.AGGREGATE_MAX.getValue(), baos.toByteArray());

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(response))) {
            return in.readDouble();
        }
    }

    public List<Event> getEvents(Set<String> products, int day) throws IOException, InterruptedException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeInt(day);
        out.writeInt(products.size()); // Envia quantos produtos vamos filtrar.
        for (String p : products) {
            out.writeUTF(p);
        }

        // Bloqueia à espera da lista...
        byte[] response = sendReceive(RequestType.FILTER_EVENTS.getValue(), baos.toByteArray());

        // Deserialização "Inteligente" (com o Dicionário de IDs que vimos no servidor).
        List<Event> events = new ArrayList<>();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(response))) {
            // 1. Lê o Dicionário (ID -> NomeProduto).
            int dictSize = in.readInt();
            Map<Integer, String> idToNameMap = new HashMap<>();
            for (int i = 0; i < dictSize; i++) {
                int id = in.readInt();
                String name = in.readUTF();
                idToNameMap.put(id, name);
            }

            // 2. Lê os Eventos usando os IDs do dicionário.
            int numEvents = in.readInt();
            for (int i = 0; i < numEvents; i++) {
                int productId = in.readInt(); // Lê ID (4 bytes) em vez do nome completo.
                int qty = in.readInt();
                double price = in.readDouble();

                String productName = idToNameMap.get(productId); // Converte ID -> Nome.
                events.add(new Event(productName, qty, price));
            }
        }
        return events;
    }

    // Notificação: O sendReceive aqui vai bloquear durante MUITO tempo (até o evento acontecer).
    public boolean waitForSimultaneousSales(String p1, String p2) throws IOException, InterruptedException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeUTF(p1);
        out.writeUTF(p2);

        // A thread do cliente fica presa aqui dentro do sendReceive -> cond.await().
        byte[] response = sendReceive(RequestType.SUB_SIMULTANEOUS.getValue(), baos.toByteArray());

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(response))) {
            return in.readBoolean();
        }
    }

    public String waitForConsecutiveSales(String product, int n) throws IOException, InterruptedException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeUTF(product);
        out.writeInt(n);

        byte[] response = sendReceive(RequestType.SUB_CONSECUTIVE.getValue(), baos.toByteArray());

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(response))) {
            boolean success = in.readBoolean();
            if (success) {
                return in.readUTF();
            } else {
                return null;
            }
        }
    }

    public void newDay() throws IOException, InterruptedException {
        sendReceive(RequestType.NEW_DAY.getValue(), new byte[0]);
    }

    @Override
    public void close() throws Exception {
        demultiplexer.close();
    }
}