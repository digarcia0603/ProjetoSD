package org.example;

import org.example.utils.Logger;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ServerWorker implements Runnable {
    private final Socket socket;
    private final TimeSeriesDB db;
    private final Demultiplexer demultiplexer;
    private boolean authenticated = false; // Estado: cliente já fez login?

    public ServerWorker(Socket socket, TimeSeriesDB db) throws IOException {
        this.socket = socket;
        this.db = db;
        // Cria o Demultiplexer para gerir a receção de pacotes deste socket.
        this.demultiplexer = new Demultiplexer(new TaggedConnection(socket));
        this.demultiplexer.start(); // Começa a thread de leitura.
    }

    @Override
    public void run() {
        try {
            while (true) {
                // Fica bloqueado à espera de uma mensagem qualquer do cliente.
                TaggedConnection.Frame frame = demultiplexer.receiveAny();

                // MULTI-THREADING NO SERVIDOR:
                // Cria uma NOVA thread para processar este pedido específico.
                // Isto permite processar vários pedidos do mesmo cliente em paralelo.
                Thread t = new Thread(() -> {
                    try {
                        handleRequest(frame);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                t.start();
            }
        } catch (Exception e) {
            Logger.log("Connection ended (Client or Error): " + e.getMessage(), Logger.LogLevel.INFO);
        } finally {
            try {
                demultiplexer.close();
                socket.close();
            } catch (IOException e) {
            }
        }
    }

    private void handleRequest(TaggedConnection.Frame frame) throws IOException {
        // Converte o ID numérico do pedido (ex: 1) para Enum (ex: REGISTER_REQUEST).
        RequestType type = RequestType.fromId(frame.requestType);

        Logger.log("ServerWorker received request ID type: " + frame.requestType + " (" + type + ")", Logger.LogLevel.INFO);

        if (type == null) return;

        // Se não estiver autenticado e tentar fazer operações proibidas, ignora.
        if (!authenticated && type != RequestType.AUTH_REQUEST && type != RequestType.REGISTER_REQUEST) {
            Logger.log("Request rejected: Client not authenticated.", Logger.LogLevel.WARN);
            return;
        }

        // Prepara streams para ler os dados que vieram na frame.
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(frame.data));
        // Prepara streams para escrever a resposta.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        // Switch gigante para decidir o que fazer.
        switch (type) {
            case AUTH_REQUEST:
                // Tenta autenticar na BD e escreve true/false na resposta.
                Logger.log("--> Processing AUTH...", Logger.LogLevel.INFO);
                boolean authResult = db.authenticate(in.readUTF(), in.readUTF());
                if (authResult) {
                    this.authenticated = true;
                }
                out.writeBoolean(authResult);
                break;
            case REGISTER_REQUEST:
                // Tenta registar novo utilizador.
                Logger.log("--> Processing REGISTER...", Logger.LogLevel.INFO);
                boolean res = db.register(in.readUTF(), in.readUTF());
                Logger.log("--> Result of the registration: " + res, Logger.LogLevel.INFO);
                out.writeBoolean(res);
                break;
            case ADD_EVENT:
                // Lê: Produto (String), Quantidade (int), Preço (double).
                // Chama db.addEvent que trata dos locks de escrita.
                db.addEvent(in.readUTF(), in.readInt(), in.readDouble());
                out.writeUTF("ACK"); // Confirmação simples.
                break;
            case NEW_DAY:
                // Avança o dia. Pode demorar se tiver de gravar em disco, mas como estamos numa thread separada, não bloqueia outros clientes.
                db.newDay();
                out.writeUTF("ACK");
                break;

            case AGGREGATE_QTY: { // Exemplo de Agregação (Quantidade Total).
                String p = in.readUTF(); // Produto.
                int d = in.readInt();    // Dias para trás.

                // Valida se o 'd' pedido não excede o histórico máximo do servidor.
                int D = db.getMaxHistoryDays();
                if (d > D) {
                    d = D;
                }

                int totalQty = 0;
                int start = db.getCurrentDay() - 1; // Começa a contar a partir de ontem.

                // Loop: Vai dia a dia buscar a agregação.
                // Isto é eficiente porque db.getAggregation usa cache e streaming.
                for (int i = 0; i < d; i++) {
                    int targetDay = start - i;
                    if (targetDay < 0) break; // Não passa do dia 0.
                    totalQty += db.getAggregation(targetDay, p).count;
                }
                out.writeInt(totalQty); // Envia o total calculado.
                break;
            }

            case AGGREGATE_VOL: {
                String p = in.readUTF();
                int d = in.readInt();
                int D = db.getMaxHistoryDays();
                if (d > D) {
                    d = D;
                }
                double totalVol = 0;
                int start = db.getCurrentDay() - 1;
                for (int i = 0; i < d; i++) {
                    int targetDay = start - i;
                    if (targetDay < 0) break;
                    totalVol += db.getAggregation(targetDay, p).volume;
                }
                out.writeDouble(totalVol);
                break;
            }
            case AGGREGATE_AVG: {
                String p = in.readUTF();
                int d = in.readInt();
                int D = db.getMaxHistoryDays();
                if (d > D) {
                    d = D;
                }
                double totalVol = 0;
                int totalCount = 0;
                int start = db.getCurrentDay() - 1;
                for (int i = 0; i < d; i++) {
                    int targetDay = start - i;
                    if (targetDay < 0) break;
                    var agg = db.getAggregation(targetDay, p);
                    totalVol += agg.volume;
                    totalCount += agg.count;
                }
                out.writeDouble(totalCount == 0 ? 0.0 : totalVol / totalCount);
                break;
            }
            case AGGREGATE_MAX: {
                String p = in.readUTF();
                int d = in.readInt();
                int D = db.getMaxHistoryDays();
                if (d > D) {
                    d = D;
                }
                double max = 0;
                int start = db.getCurrentDay() - 1;
                for (int i = 0; i < d; i++) {
                    int targetDay = start - i;
                    if (targetDay < 0) break;
                    double m = db.getAggregation(targetDay, p).maxPrice;
                    if (m > max) max = m;
                }
                out.writeDouble(max);
                break;
            }
            
            case FILTER_EVENTS: { // Listagem de Eventos (Pesada!).
                int daysAgo = in.readInt();
                
                int numProds = in.readInt();
                Set<String> targets = new HashSet<>();
                for(int i=0; i<numProds; i++) targets.add(in.readUTF()); // Lê lista de produtos a filtrar.
                
                int D = db.getMaxHistoryDays();
                // Validações básicas.
                if (daysAgo < 1 || daysAgo > D) {
                    out.writeInt(0); // 0 nomes.
                    out.writeInt(0); // 0 eventos.
                    break;
                }
                
                int targetDay = db.getCurrentDay() - daysAgo;

                // Obtém o mapa completo do dia (pode vir do disco).
                Map<String, List<Event>> dayData = db.getDayData(targetDay);

                // OTIMIZAÇÃO (Compressão de Nomes):
                // Para não enviar a string "ComputadorXPTO" mil vezes, criamos um ID temporário.
                Map<String, Integer> nameToId = new HashMap<>();
                int nextId = 1;
                List<Event> eventsToSend = new ArrayList<>();

                // Filtra apenas os eventos dos produtos pedidos.
                for (String pName : targets) {
                    List<Event> evs = dayData.get(pName);
                    if (evs != null) {
                        if (!nameToId.containsKey(pName)) nameToId.put(pName, nextId++);
                        eventsToSend.addAll(evs);
                    }
                }

                // 1. Envia o Dicionário (ID -> Nome).
                out.writeInt(nameToId.size());
                for(var entry : nameToId.entrySet()) {
                    out.writeInt(entry.getValue());
                    out.writeUTF(entry.getKey());
                }
                // 2. Envia os Eventos usando o ID em vez do Nome.
                out.writeInt(eventsToSend.size());
                for (Event e : eventsToSend) {
                    out.writeInt(nameToId.get(e.getProduct()));
                    out.writeInt(e.getQuantity());
                    out.writeDouble(e.getPrice());
                }
                break;
            }
            
            case SUB_SIMULTANEOUS: { // Notificação Bloqueante.
                String p1 = in.readUTF();
                String p2 = in.readUTF();

                // Prepara o bloqueio local.
                ReentrantLock lock = new ReentrantLock();
                Condition cond = lock.newCondition();
                TimeSeriesDB.WaitingClient wc = new TimeSeriesDB.WaitingClient(lock, cond);

                // Regista na BD: "Estou à espera disto, acorda este 'wc' quando acontecer".
                db.registerSimultaneousWait(p1, p2, wc);
                
                lock.lock();
                try {
                    // BLOQUEIA A THREAD AQUI.
                    // Enquanto wc.satisfied for false, esta thread dorme e não gasta CPU.
                    while (!wc.satisfied) {
                        cond.await();
                    }
                } catch (InterruptedException e) {
                    wc.satisfied = false;
                } finally {
                    lock.unlock();
                }
                // Quando acorda (seja por sucesso ou newDay), responde.
                out.writeBoolean(wc.satisfied);
                break;
            }
            case SUB_CONSECUTIVE: {
                // Lógica idêntica à de cima, mas envia também o nome do produto no final.
                String p = in.readUTF();
                int n = in.readInt();

                ReentrantLock lock = new ReentrantLock();
                Condition cond = lock.newCondition();
                TimeSeriesDB.WaitingClient wc = new TimeSeriesDB.WaitingClient(lock, cond);

                db.registerConsecutiveWait(p, n, wc);

                lock.lock();
                try {
                    while (!wc.satisfied) cond.await();
                } catch (InterruptedException e) {
                    wc.satisfied = false;
                } finally {
                    lock.unlock();
                }

                out.writeBoolean(wc.satisfied);
                if (wc.satisfied) {
                    // Se teve sucesso, diz qual foi o produto (útil se o pedido fosse genérico, embora aqui o cliente saiba o que pediu).
                    out.writeUTF(wc.resultData != null ? wc.resultData : "");
                }
                break;
            }

            case DISCONNECT:
                demultiplexer.close(); // Fecha a ligação do lado do servidor.
                break;
        }

        // FASE FINAL: Envio da Resposta.
        // Usa o Demultiplexer para enviar a resposta de forma segura (thread-safe).
        // IMPORTANTE: Usa frame.tag! Isto garante que o cliente sabe que esta resposta
        // corresponde àquele pedido específico.
        demultiplexer.send(frame.tag, type.getValue(), baos.toByteArray());
    }
}