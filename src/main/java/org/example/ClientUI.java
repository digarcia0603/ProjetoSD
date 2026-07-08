package org.example;

import org.example.utils.Logger;

import java.util.*;

public class ClientUI {
    public static void main(String[] args) {
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Connecting to server...");
            // Cria a instância da biblioteca (abre socket TCP).
            ClientLibrary client = new ClientLibrary("localhost", 12345);
            Logger.log("Connected!", Logger.LogLevel.INFO);

            // --- FASE 1: Loop de Autenticação ---
            boolean authenticated = false;
            while (!authenticated) {
                System.out.println("1. Register");
                System.out.println("2. Login");
                System.out.println("3. Quit");
                System.out.print("Choice: ");

                int choice = readInt(scanner);
                if (choice == 3) {
                    client.close();
                    return;
                }

                System.out.print("Username: ");
                String user = scanner.nextLine();
                System.out.print("Password: ");
                String pass = scanner.nextLine();

                if (choice == 1) {
                    // Chama registo síncrono.
                    if (client.register(user, pass)) {
                        System.out.println(">> Registration completed successfully! Please log in.");
                    } else {
                        System.out.println(">> Error: User already exists.");
                    }
                } else if (choice == 2) {
                    // Chama login síncrono.
                    if (client.authenticate(user, pass)) {
                        System.out.println(">> Login completed successfully!");
                        authenticated = true;
                    } else {
                        System.out.println(">> Error: Invalid credentials.");
                    }
                }
            }

            // --- FASE 2: Menu Principal ---
            boolean running = true;
            List<Thread> activeThreads = new ArrayList<>(); // Guarda threads de notificação.

            while (running) {
                System.out.println("\n=== MENU PRINCIPAL ===");
                System.out.println("1. Add Event (Sale)");
                System.out.println("2. New Day");
                System.out.println("3. Check info aggregation (Qty, Vol€, Avg€, Max€)");
                System.out.println("4. Filter events (List)");
                System.out.println("5. Notify two product sales today (Simultaneous sales)");
                System.out.println("6. Notify n consecutive sales today from a product (Consecutive sales)");
                System.out.println("0. Quit");
                System.out.print("Option: ");

                int op = readInt(scanner);

                switch(op) {
                    case 1:
                        System.out.print("Product: ");
                        String prod = scanner.nextLine();
                        System.out.print("Quantity: ");
                        int qty = readInt(scanner);
                        System.out.print("Price: ");
                        double price = readDouble(scanner);

                        try {
                            // Chama o método da lib. A UI bloqueia por milissegundos até receber o ACK.
                            client.addEvent(prod, qty, price);
                            System.out.println("[Info] Sale recorded: " + prod);
                        } catch (Exception e) {
                            System.out.println("[Error] Failed to insert: " + e.getMessage());
                        }
                        break;

                    case 2:
                        try {
                            System.out.println("[System] Asking for day change...");
                            client.newDay();
                            System.out.println("[System] Server advanced to the next day.");
                        } catch (Exception e) {
                            System.out.println("[Error] New Day: " + e.getMessage());
                        }
                        break;

                    case 3:
                        System.out.println("--- Info aggregation ---");
                        System.out.println("1. Total Quantity");
                        System.out.println("2. Sale Volume (€)");
                        System.out.println("3. Average Price");
                        System.out.println("4. Max Price");
                        System.out.print("> ");
                        int aggType = readInt(scanner);

                        System.out.println("Product: ");
                        String pAgg = scanner.nextLine();
                        System.out.print("Last N days: ");
                        int days = readInt(scanner);

                        try {
                            String res = "";
                            switch (aggType) {
                                case 1: res = "" + client.getSoldQuantity(pAgg, days); break;
                                case 2: res = String.format("%.2f €", client.getSalesVolume(pAgg, days)); break;
                                case 3: res = String.format("%.2f €", client.getAveragePrice(pAgg, days)); break;
                                case 4: res = String.format("%.2f €", client.getMaxPrice(pAgg, days)); break;
                                default: res = "Invalid option";
                            }
                            System.out.println("[Result] Info Aggregation (" + pAgg + "): " + res);
                        } catch (Exception e) {
                            System.out.println("[Error] Info Aggregation: " + e.getMessage());
                        }
                        break;

                    case 4:
                        System.out.print("How many days ago (1 to \"D\")?: ");
                        int targetDay= readInt(scanner);
                        System.out.print("Products (separated by commas): ");
                        String line = scanner.nextLine();
                        Set<String> products = new HashSet<>(Arrays.asList(line.split(",")));

                        Set<String> cleanProducts = new HashSet<>();
                        for(String s : products) cleanProducts.add(s.trim());

                        try {
                            List<Event> events = client.getEvents(cleanProducts, targetDay);
                            System.out.println("\n--- Events (" + targetDay + " days ago) ---");
                            if (events.isEmpty()) System.out.println("No event found.");
                            for (Event e : events) {
                                System.out.println(e.toString());
                            }
                            System.out.println("---------------------------");
                        } catch (Exception e) {
                            System.out.println("[Error] List: " + e.getMessage());
                        }
                        break;

                    case 5:
                        System.out.print("Product 1: ");
                        String p1 = scanner.nextLine();
                        System.out.print("Product 2: ");
                        String p2 = scanner.nextLine();

                        System.out.println("[System] Waiting for simultaneous sales (" + p1 + ", " + p2 + ")...");

                        // CRUCIAL PARA A DEFESA:
                        // Se chamássemos client.waitForSimultaneousSales() diretamente aqui,
                        // o menu ficava congelado e não podíamos fazer mais nada!
                        // Por isso, criamos uma NOVA THREAD dedicada a esperar.
                        Thread tSim = new Thread(() -> {
                            try {
                                boolean result = client.waitForSimultaneousSales(p1, p2);
                                if (result) System.out.println("\n*** NOTIFICATION: " + p1 + " and " + p2 + " sold today! ***");
                                else System.out.println("\n[Info] Day finished without simultaneous sale of " + p1 + " and " + p2);
                            } catch (Exception e) {
                                System.out.println("\n[Error] Simultaneous sale notification interrupted.");
                            }
                        });
                        tSim.start();
                        activeThreads.add(tSim);
                        break;

                    case 6:
                        System.out.print("Product: ");
                        String pCons = scanner.nextLine();
                        System.out.print("Number of Sales: ");
                        int nCons = readInt(scanner);

                        System.out.println("[System] Waiting for " + nCons + " consecutive sales of " + pCons + "...");

                        Thread tCons = new Thread(() -> {
                            try {
                                String result = client.waitForConsecutiveSales(pCons, nCons);
                                if (result != null) System.out.println("\n*** NOTIFICATION: " + nCons + " consecutive sales of " + result + "! ***");
                                else System.out.println("\n[Info] Day finished with no sequence for " + pCons);
                            } catch (Exception e) {
                                System.out.println("\n[Error] Consecutive sale notification interrupted.");
                            }
                        });
                        tCons.start();
                        activeThreads.add(tCons);
                        break;

                    case 0:
                        running = false;
                        client.close();
                        break;

                    default:
                        System.out.println("Invalid option.");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int readInt(Scanner s) {
        while (true) {
            try {
                int i = s.nextInt();
                s.nextLine();
                return i;
            } catch (InputMismatchException e) {
                s.nextLine();
                System.out.print(">> Invalid value! Write an int number: ");
            }
        }
    }

    private static double readDouble(Scanner s) {
        while (true) {
            try {
                double d = s.nextDouble();
                s.nextLine();
                return d;
            } catch (InputMismatchException e) {
                s.nextLine();
                System.out.print(">> Invalid value! Write a decimal number (e.g. 10,5): ");
            }
        }
    }
}