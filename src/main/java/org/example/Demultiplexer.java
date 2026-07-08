package org.example;

import javax.swing.text.html.HTML;
import java.io.EOFException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

// Distribui as mensagens recebidas para quem as pediu.
public class Demultiplexer implements AutoCloseable {
    private final TaggedConnection conn;
    // Mapa de filas de espera: TAG -> Fila de Frames.
    private final Map<Integer, BlockingQueue<TaggedConnection.Frame>> queues = new ConcurrentHashMap<>();
    private final Thread readerThread; // Thread dedicada a ler do socket.
    private volatile boolean closed = false;
    private ClientLibrary clientLibrary = null; // Referência para a biblioteca (se estivermos no cliente).
    private final ReentrantLock startLock = new ReentrantLock();

    public Demultiplexer(TaggedConnection conn) {
        this.conn = conn;
        this.readerThread = new Thread(this::reader); // Cria a thread que executa o método 'reader'.
    }

    public void setClientLibrary(ClientLibrary clientLibrary) {
        this.clientLibrary = clientLibrary;
    }

    public void start() {
        startLock.lock();
        try {
            if (!readerThread.isAlive()) {
                readerThread.start();
            }
        } finally {
            startLock.unlock();
        }
    }

    // Método que corre em loop na thread separada.
    private void reader() {
        try {
            while (!closed) {
                try {
                    // Lê a próxima frame do socket (bloqueia aqui se não houver dados).
                    TaggedConnection.Frame frame = conn.receive();

                    // Se estivermos no Cliente:
                    if (clientLibrary != null) {
                        // Avisa a biblioteca que chegou uma resposta para a tag X.
                        clientLibrary.addResponse(frame.tag, frame.data);
                    } else {
                        // Se estivermos no Servidor:
                        // Encontra a fila correspondente à TAG e mete lá a frame.
                        // Se a fila não existir, cria uma nova.
                        BlockingQueue<TaggedConnection.Frame> queue = queues.computeIfAbsent(frame.tag, k-> new ArrayBlockingQueue<>(1024));
                        queue.put(frame);
                    }
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (IOException | InterruptedException e) {
            if (!closed) e.printStackTrace();
        } finally {
            queues.clear();
        }
    }

    public void send(int tag, short request, byte[] data) throws IOException {
        conn.send(tag, request, data);
    }

    // Usado pelo ServidorWorker para ir buscar a próxima mensagem de uma TAG específica (ou qualquer uma).
    public TaggedConnection.Frame receiveAny() throws InterruptedException {
        while (!closed) {
            // Percorre todas as filas à procura de alguma frame.
            for (Map.Entry<Integer, BlockingQueue<TaggedConnection.Frame>> entry : queues.entrySet()) {
                BlockingQueue<TaggedConnection.Frame> queue = entry.getValue(); // Tenta tirar da fila.
                TaggedConnection.Frame frame = queue.poll();

                if (frame != null) {
                    return frame; // Se encontrou, devolve.
                }
            }
            Thread.sleep(10); // Espera um pouco para não gastar 100% CPU.
        }
        throw new InterruptedException("Demultiplexer is closed");
    }

    @Override
    public void close() throws IOException {
        closed = true;
        readerThread.interrupt();
        conn.close();
    }
}