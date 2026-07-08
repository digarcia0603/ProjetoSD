import org.example.ClientLibrary;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import javax.swing.*;
import java.awt.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class TimeSeriesRunner {
    private static final String HOST = "localhost";
    private static final int PORT = 12345;
    public int maxClients;

    public static void main(String[] args) {
        try {
            TimeSeriesRunner runner = new TimeSeriesRunner();

            if (!runner.checkConnection()) {
                System.err.println("Error: The server doesn't seem to be running in" + HOST + ":" + PORT);
                return;
            }

            try (Scanner scanner = new Scanner(System.in)) {
                System.out.print("Maximum number of simultaneous clients:\n|> ");
                runner.maxClients = scanner.nextInt();

                System.out.println("Choose the Workload (Test Scenario):");
                System.out.println("1. Massive ingestion (1000 random events)");
                System.out.println("2. Hotspot Ingestion (1000 events on the same product)");
                System.out.println("3. Intensive Reading (Heavy aggregations)");
                System.out.print("|> ");
                int workload = scanner.nextInt();

                switch (workload) {
                    case 1: runner.workload1_RandomInsert(); break;
                    case 2: runner.workload2_HotspotInsert(); break;
                    case 3: runner.workload3_HeavyAggregation(); break;
                    default: System.out.println("Invalid option.");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean checkConnection() {
        try (ClientLibrary ignored = new ClientLibrary(HOST, PORT)) {
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public void workload1_RandomInsert() {
        System.out.println(">>>  Running Workload 1: Random Product Ingestion...");
        runBenchmark("Random Ingestion", (clientId, iteration) -> {
            String prod = "Prod_" + clientId + "_" + iteration;
            addEvent(prod, 1, 10.0);
        });
    }

    public void workload2_HotspotInsert() {
        System.out.println(">>> Running Workload 2: Hotspot Ingesion (Same Product)...");
        runBenchmark("Hotspot Ingestion", (clientId, iteration) -> {
            addEvent("Popular_Product", 1, 50.0);
        });
    }

    public void workload3_HeavyAggregation() {
        System.out.println(">>> Running Workload 3: Aggregations (Reading)");
        System.out.println("   (Populating initial data...)");

        try {
            for(int i=0; i<100; i++) {
                addEvent("Product_X", 10, 5.0);
            }
        } catch(Exception e) {
            System.err.println("Error populating initial data: " + e.getMessage());
        }

        runBenchmark("Aggregation (Query)", (clientId, iteration) -> {
            getSoldQuantity("Product_X", 5);
        });
    }

    @FunctionalInterface
    private interface Task {
        void run(int clientId, int iteration) throws Exception;
    }

    private void runBenchmark(String title, Task task) {
        ExecutorService executor = Executors.newFixedThreadPool(maxClients);
        List<Long> responseTimes = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        ReentrantLock dataLock = new ReentrantLock();

        long testStart = System.currentTimeMillis();

        int totalOps = 1000;
        int calculatedOps = totalOps / maxClients;
        final int opsPerClient = (calculatedOps == 0) ? 1 : calculatedOps;

        for (int i = 0; i < maxClients; i++) {
            final int id = i;
            executor.submit(() -> {
                for (int j = 0; j < opsPerClient; j++) {
                    long opStart = System.nanoTime();
                    try {
                        task.run(id, j);
                        long duration = System.nanoTime() - opStart;
                        long stamp = System.currentTimeMillis() - testStart;

                        dataLock.lock();
                        try {
                            responseTimes.add(duration);
                            timestamps.add(stamp);
                        } finally {
                            dataLock.unlock();
                        }
                    } catch (Exception e) {
                        System.err.println("Error in operation (Client " + id + "): " + e.getMessage());
                    }
                }
            });
        }

        executor.shutdown();
        try {
            boolean finished = executor.awaitTermination(2, TimeUnit.MINUTES);
            if (!finished) System.err.println("Warning: Benchmark exceded the time limit.");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        generateGraph(responseTimes, timestamps,
                title + " (" + maxClients + " Clients)",
                "Latency of operations over time");
    }

    private void addEvent(String prod, int qty, double price) throws Exception {
        try (ClientLibrary client = new ClientLibrary(HOST, PORT)) {
            client.register("test_user", "password");
            client.authenticate("test_user", "password");

            client.addEvent(prod, qty, price);
        }
    }

    private void getSoldQuantity(String prod, int days) throws Exception {
        try (ClientLibrary client = new ClientLibrary(HOST, PORT)) {
            client.register("test_user", "password");
            client.authenticate("test_user", "password");

            client.getSoldQuantity(prod, days);
        }
    }

    public void generateGraph(List<Long> responseTimes, List<Long> timestamps, String title, String subtitle) {
        if (responseTimes.isEmpty()) {
            System.out.println("No data available to generate chat.");
            return;
        }

        XYSeries series = new XYSeries("Response Time");
        for (int i = 0; i < responseTimes.size(); i++) {
            series.add(timestamps.get(i).doubleValue(), responseTimes.get(i) / 1_000_000.0);
        }

        XYSeriesCollection dataset = new XYSeriesCollection(series);
        JFreeChart chart = ChartFactory.createXYLineChart(
                title,
                "Elapsed time (ms)",
                "Latency (ms)",
                dataset
        );

        if (subtitle != null) chart.addSubtitle(new TextTitle(subtitle));

        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame("Test Results");
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            ChartPanel panel = new ChartPanel(chart);
            panel.setPreferredSize(new Dimension(800, 600));
            frame.add(panel);
            frame.pack();
            frame.setLocationRelativeTo(null);
            frame.setVisible(true);
        });
        System.out.println("Chart generated!");
    }
}