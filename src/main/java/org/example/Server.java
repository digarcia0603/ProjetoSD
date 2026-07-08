package org.example;

import org.example.utils.Logger;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
    public static void main(String[] args) {
        int port = 12345; // Define a porta onde o servidor vai escutar (12345).

        final int MAX_DAYS_IN_MEMORY = 3; // Define quantos dias ficam na RAM (parametro S).

        int maxHistoryDays = 30; // Define o histórico total disponível (parametro D), por defeito 30.

        // Verifica se o utilizador passou um argumento (ex: java Server 60)
        if (args.length > 0) {
            try {
                // Tenta converter o argumento para inteiro para definir o novo D.
                maxHistoryDays = Integer.parseInt(args[0]);

                // Aviso de segurança se a cache for maior que o histórico (S > D não faz sentido).
                if (MAX_DAYS_IN_MEMORY >= maxHistoryDays) {
                    System.err.println("Critical Warning: S < D");
                    System.err.println("S (Cache) = " + MAX_DAYS_IN_MEMORY);
                    System.err.println("D (History) = " + maxHistoryDays);
                }
            } catch (NumberFormatException e) {
                // Se o argumento não for um número, avisa e termina.
                System.err.println("Error: Argument D should be an int number.");
                return;
            }
        }

        // Imprime as configurações iniciais no terminal.
        System.out.println(">>> SERVER STARTED <<<");
        System.out.println("  S (Default Cache) = " + MAX_DAYS_IN_MEMORY);
        System.out.println("  D (History)       = " + maxHistoryDays);
        System.out.println("-----------------------------");

        // Cria a instância da Base de Dados com os limites definidos.
        TimeSeriesDB db = new TimeSeriesDB(MAX_DAYS_IN_MEMORY, maxHistoryDays);

        // Bloco try-with-resources para criar o socket do servidor (fecha automaticamente se der erro).
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            Logger.log("Listening on door " + port, Logger.LogLevel.INFO); // Log de arranque.

            // Loop infinito: o servidor nunca para de aceitar clientes.
            while (true) {
                // Bloqueia aqui até um cliente tentar conectar-se. Retorna um socket para esse cliente.
                Socket clientSocket = serverSocket.accept();
                Logger.log("New client: " + clientSocket.getInetAddress(), Logger.LogLevel.INFO);

                // Cria um Worker (trabalhador) para lidar com este cliente específico.
                ServerWorker worker = new ServerWorker(clientSocket, db);

                // Inicia o Worker numa nova Thread para não bloquear o servidor principal.
                new Thread(worker).start();
            }
        } catch (IOException e) {
            e.printStackTrace(); // Imprime erros de rede (ex: porta ocupada).
        }
    }
}