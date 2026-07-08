package org.example;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// Encapsula o Socket e define o formato das mensagens.
public class TaggedConnection implements AutoCloseable {
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    // Locks para garantir que só uma thread escreve/lê no socket de cada vez.
    private final Lock sendLock = new ReentrantLock();
    private final Lock receiveLock = new ReentrantLock();

    // Classe interna que representa o pacote de dados (Tag + Tipo + Dados).
    public static class Frame {
        public final int tag;
        public final short requestType;
        public final byte[] data;

        public Frame(int tag, short requestType, byte[] data) {
            this.tag = tag;
            this.requestType = requestType;
            this.data = data;
        }
    }

    public TaggedConnection(Socket socket) throws IOException {
        this.socket = socket;
        // Cria streams com buffer para melhor performance.
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    public void send(Frame frame) throws IOException {
        send(frame.tag, frame.requestType, frame.data);
    }

    // Envia uma mensagem.
    public void send(int tag, short request, byte[] data) throws IOException {
        sendLock.lock(); // Garante exclusão mútua na escrita.
        try {
            out.writeInt(tag);        // 1. Escreve a TAG.
            out.writeShort(request);  // 2. Escreve o TIPO.
            out.writeInt(data.length); // 3. Escreve o TAMANHO dos dados.
            out.write(data);          // 4. Escreve os DADOS.
            out.flush(); // Força o envio imediato.
        } finally {
            sendLock.unlock();
        }
    }

    // Recebe uma mensagem.
    public Frame receive() throws IOException {
        receiveLock.lock(); // Garante exclusão mútua na leitura.
        try {
            int tag = in.readInt();
            short request = in.readShort();
            int length = in.readInt();
            byte[] data = new byte[length];
            in.readFully(data); // Lê exatamente 'length' bytes.
            return new Frame(tag, request, data);
        } finally {
            receiveLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}