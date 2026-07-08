package org.example;

import org.example.utils.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TimeSeriesDB {
    private final int maxDaysInMemory; // Limite S (RAM).
    private final int maxHistoryDays; // Limite D (Total).
    private final String dataDirectory = "data_store"; // Pasta onde guarda ficheiros.

    // Map seguro para guardar users e passwords (User -> Pass).
    private final Map<String, String> users = new ConcurrentHashMap<>();
    private final ReentrantLock userLock = new ReentrantLock(); // Lock exclusivo para registos/logins.

    private int currentDay = 0; // Contador do dia atual (começa em 0).

    // A RAM: Mapa de (Dia -> (Produto -> Lista de Eventos)).
    private final Map<Integer, Map<String, List<Event>>> daysInMemory = new ConcurrentHashMap<>();

    // Regista quais os dias que foram alterados e precisam de ser gravados em disco.
    private final Set<Integer> dirtyDays = ConcurrentHashMap.newKeySet();

    // Cache para resultados de agregações antigas (Dia:Produto -> Resultado).
    private final Map<String, AggregationCache> aggregationCache = new ConcurrentHashMap<>();

    // O TEU SEGREDO DE DEFESA: Um lock ReadWrite por cada dia!
    private final Map<Integer, ReentrantReadWriteLock> dayLocks = new ConcurrentHashMap<>();
    // Lock global apenas para mudar de dia ou limpar memória (operações estruturais).
    private final ReentrantLock globalLock = new ReentrantLock();

    // Listas de espera para clientes bloqueados (Notificações).
    private final Map<String, List<WaitingClient>> simultaneousWaiters = new ConcurrentHashMap<>();
    private final Map<String, List<WaitingClient>> consecutiveWaiters = new ConcurrentHashMap<>();

    // Auxiliares para verificar as notificações rapidamente.
    private final Set<String> productsSoldToday = ConcurrentHashMap.newKeySet(); // O que já foi vendido hoje.
    private final Map<String, Integer> currentConsecutiveCounts = new ConcurrentHashMap<>(); // Contador de sequências.
    private String lastGlobalProduct = null; // Último produto vendido globalmente.

    private static final int CACHE_HISTORY_LIMIT = 30; // Limite extra para a cache de agregações.

    // Classe interna simples para guardar resultados de querys.
    public static class AggregationCache {
        int count = 0;
        double volume = 0.0;
        double maxPrice = 0.0;
    }

    // Classe para representar um cliente que está bloqueado à espera de notificação.
    public static class WaitingClient {
        public final Condition cond; // A condição onde ele dorme.
        public final ReentrantLock lock; // O lock associado à condição.
        public boolean satisfied = false; // Flag para saber se acordou com sucesso.
        public String resultData = null; // Dados extra (ex: nome do produto).

        public WaitingClient(ReentrantLock lock, Condition cond) {
            this.lock = lock;
            this.cond = cond;
        }
    }

    // Construtor: Inicializa a BD.
    public TimeSeriesDB(int maxDaysInMemory, int maxHistoryDays) {
        this.maxDaysInMemory = maxDaysInMemory;
        this.maxHistoryDays = maxHistoryDays;
        new File(dataDirectory).mkdirs(); // Cria a pasta "data_store" se não existir.

        // Inicializa o Dia 0 em memória.
        daysInMemory.put(0, new HashMap<>());
        dayLocks.put(0, new ReentrantReadWriteLock());
    }

    public int getMaxHistoryDays() {
        return maxHistoryDays;
    }

    // Método para registar utilizador.
    public boolean register(String user, String pass) {
        userLock.lock(); // Bloqueia para garantir que não há registos duplicados em simultâneo.
        try {
            if (users.containsKey(user)) return false; // Se já existe, falha.
            users.put(user, pass);
            return true;
        } finally {
            userLock.unlock(); // Liberta o lock sempre.
        }
    }

    // Método de login.
    public boolean authenticate(String user, String pass) {
        userLock.lock(); // Bloqueia para ler consistentemente.
        try {
            // Verifica se existe e se a password bate certo.
            return users.containsKey(user) && users.get(user).equals(pass);
        } finally {
            userLock.unlock();
        }
    }

    // Adicionar uma venda (Acontece no dia atual).
    public void addEvent(String product, int qty, double price) {
        ReentrantReadWriteLock lock = getDayLock(currentDay); // Pede o lock do dia de hoje.
        lock.writeLock().lock(); // Lock de ESCRITA (exclusivo). Ninguém lê nem escreve no dia atual enquanto isto corre.
        try {
            // Vai buscar o mapa de eventos do dia atual.
            Map<String, List<Event>> todayEvents = daysInMemory.get(currentDay);
            // Se o produto não existe no mapa, cria uma lista vazia.
            todayEvents.putIfAbsent(product, new ArrayList<>());
            // Adiciona o evento à lista.
            todayEvents.get(product).add(new Event(product, qty, price));

            dirtyDays.add(currentDay); // Marca o dia como "sujo" (tem dados novos por gravar).
            productsSoldToday.add(product); // Marca que este produto foi vendido (para notificações).

            // Lógica para verificar vendas consecutivas globais:
            if (product.equals(lastGlobalProduct)) {
                currentConsecutiveCounts.merge(product, 1, Integer::sum); // Incrementa.
            } else {
                currentConsecutiveCounts.clear(); // Quebrou a sequência! Limpa tudo.
                currentConsecutiveCounts.put(product, 1); // Começa nova contagem.
                lastGlobalProduct = product;
            }

            // Verifica se alguém deve ser acordado.
            checkSimultaneous(product);
            checkConsecutive(product);

        } finally {
            lock.writeLock().unlock(); // Liberta o lock.
        }
    }

    // Verifica notificações de "Vendas Simultâneas".
    private void checkSimultaneous(String product) {
        // Percorre todos os pedidos de notificação.
        for (Map.Entry<String, List<WaitingClient>> entry : simultaneousWaiters.entrySet()) {
            String[] parts = entry.getKey().split(":"); // A chave é "ProdA:ProdB".
            // Se o produto vendido completa o par que alguém espera...
            if ((product.equals(parts[0]) && productsSoldToday.contains(parts[1])) ||
                    (product.equals(parts[1]) && productsSoldToday.contains(parts[0]))) {
                signalWaiters(entry.getValue()); // Acorda esses clientes!
            }
        }
    }


    // Verifica se a venda atual completou alguma sequência de vendas consecutivas.
    private void checkConsecutive(String product) {
        // Vai buscar quantas vezes este produto já foi vendido consecutivamente HOJE.
        // (Este mapa 'currentConsecutiveCounts' é atualizado no método addEvent).
        int count = currentConsecutiveCounts.getOrDefault(product, 0);

        // Lista temporária para guardar chaves que já foram satisfeitas e podem ser removidas.
        List<String> toRemove = new ArrayList<>();

        // Percorre todos os clientes que estão à espera de vendas consecutivas.
        for (Map.Entry<String, List<WaitingClient>> entry : consecutiveWaiters.entrySet()) {
            String[] parts = entry.getKey().split(":"); // A chave é "Produto:Numero" (ex: "Caneta:3").

            // Se o produto for o mesmo e a contagem atual for maior ou igual ao pedido...
            if (parts[0].equals(product) && count >= Integer.parseInt(parts[1])) {
                // Acorda todos os clientes que estavam à espera disto.
                for(WaitingClient wc : entry.getValue()) {
                    wc.lock.lock(); // Obtém o lock do cliente.
                    try {
                        wc.satisfied = true; // Sucesso!
                        wc.resultData = product; // Informa qual foi o produto (útil para o cliente).
                        wc.cond.signal(); // ACORDA A THREAD!
                    } finally {
                        wc.lock.unlock();
                    }
                }
                // Marca esta entrada para remoção (já foram notificados).
                toRemove.add(entry.getKey());
            }
        }
        // Limpa as entradas satisfeitas do mapa de espera.
        for(String k : toRemove) consecutiveWaiters.remove(k);
    }

    // Acorda uma lista de clientes.
    private void signalWaiters(List<WaitingClient> waiters) {
        Iterator<WaitingClient> it = waiters.iterator();
        while (it.hasNext()) {
            WaitingClient wc = it.next();
            wc.lock.lock(); // Precisa do lock do cliente para usar a condition.
            try {
                wc.satisfied = true; // Marca como sucesso.
                wc.cond.signal(); // ACORDA A THREAD!
            } finally {
                wc.lock.unlock();
            }
            it.remove(); // Remove da lista de espera.
        }
    }

    // Regista um cliente à espera de vendas simultâneas.
    public void registerSimultaneousWait(String p1, String p2, WaitingClient wc) {
        // Se JÁ aconteceu hoje, acorda logo e não espera.
        if (productsSoldToday.contains(p1) && productsSoldToday.contains(p2)) {
            wc.lock.lock();
            try {
                wc.satisfied = true;
                wc.cond.signal();
            } finally {
                wc.lock.unlock();
            }
            return;
        }
        // Se não, adiciona à lista para esperar.
        simultaneousWaiters.computeIfAbsent(p1 + ":" + p2, k -> new ArrayList<>()).add(wc);
    }

    // Regista um cliente à espera de N vendas consecutivas.
    public void registerConsecutiveWait(String p, int n, WaitingClient wc) {
        // Adiciona à lista de espera. A chave é "NomeProduto:Quantidade".
        // computeIfAbsent cria a lista se ela ainda não existir.
        consecutiveWaiters.computeIfAbsent(p + ":" + n, k -> new ArrayList<>()).add(wc);
    }

    // Passagem de Dia (Manutenção e Persistência).
    public void newDay() throws IOException {
        globalLock.lock(); // Lock Global: Ninguém entra na DB durante a mudança de dia.
        try {
            wakeUpAllFailedWaiters(); // Cancela quem estava à espera de notificações (o dia acabou).

            // Se o dia atual teve alterações, grava no disco.
            if (dirtyDays.contains(currentDay)) {
                Logger.log("NewDay: Persisting Day " + currentDay + " on disk.", Logger.LogLevel.INFO);
                saveDayToDisk(currentDay);
                dirtyDays.remove(currentDay); // Já está limpo.
            }

            // Limpa estruturas auxiliares do dia.
            productsSoldToday.clear();
            currentConsecutiveCounts.clear();

            // Se a RAM está cheia (S dias), expulsa um dia antigo.
            if(daysInMemory.size() >= maxDaysInMemory) {
                evictOneDay();
            }

            cleanUpCache(); // Limpa cache de queries muito antigas.

            currentDay++; // Avança o ponteiro do tempo.
            Logger.log("Starting Day " + currentDay, Logger.LogLevel.INFO);

            // Prepara o novo dia vazio.
            daysInMemory.put(currentDay, new HashMap<>());
            dayLocks.put(currentDay, new ReentrantReadWriteLock());

        } finally {
            globalLock.unlock();
        }
    }

    // Chamado no 'newDay' para limpar todas as esperas pendentes.
    private void wakeUpAllFailedWaiters() {
        // Acorda quem esperava vendas simultâneas.
        for (List<WaitingClient> l : simultaneousWaiters.values()) signalFailed(l);
        simultaneousWaiters.clear(); // Limpa o mapa.

        // Acorda quem esperava vendas consecutivas.
        for (List<WaitingClient> l : consecutiveWaiters.values()) signalFailed(l);
        consecutiveWaiters.clear(); // Limpa o mapa.
    }

    // Helper para acordar uma lista de clientes dizendo que FALHARAM.
    private void signalFailed(List<WaitingClient> list) {
        for(WaitingClient wc : list) {
            wc.lock.lock();
            try {
                wc.satisfied = false; // Define como falha (o dia acabou sem o evento acontecer).
                wc.cond.signal(); // Acorda a thread (que vai ver que satisfied = false).
            } finally {
                wc.lock.unlock();
            }
        }
    }

    // Remove entradas da cache que já são demasiado antigas (passado remoto).
    private void cleanUpCache() {
        // Define o limite: Dia Atual - 30 dias (exemplo).
        int limit = currentDay - CACHE_HISTORY_LIMIT;

        // Remove do mapa se a chave (Dia) for menor que o limite.
        aggregationCache.keySet().removeIf(k -> {
            try {
                // A chave é "Dia:Produto", fazemos split para obter o dia.
                return Integer.parseInt(k.split(":")[0]) < limit;
            } catch (Exception e) {
                return true; // Se der erro no parse, remove por segurança.
            }
        });
    }

    // Expulsa o dia mais antigo da RAM para o Disco.
    private void evictOneDay() throws IOException {
        int candidate = -1;
        int minDay = Integer.MAX_VALUE;

        // Procura a chave (dia) mais pequena que não seja o dia atual.
        for (int d : daysInMemory.keySet()) {
            if (d != currentDay && d < minDay) {
                minDay = d;
                candidate = d;
            }
        }

        if (candidate != -1) {
            // Se esse dia não foi gravado, grava agora.
            if (dirtyDays.contains(candidate)) {
                Logger.log("Evict: Saving day " + candidate + " on disk (Safety).", Logger.LogLevel.DEBUG);
                saveDayToDisk(candidate);
                dirtyDays.remove(candidate);
            } else {
                Logger.log("Evict: Removing day " + candidate + " from RAM.", Logger.LogLevel.DEBUG);
            }
            // Apaga da RAM.
            daysInMemory.remove(candidate);
        }
    }

    // Grava um dia num ficheiro binário (.dat).
    private void saveDayToDisk(int day) throws IOException {
        Map<String, List<Event>> data = daysInMemory.get(day);
        if (data == null) return;

        File f = new File(dataDirectory, "day_" + day + ".dat");
        // Usa BufferedOutputStream para performance.
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)))) {
            dos.writeInt(data.size()); // 1. Escreve número de produtos.
            for (Map.Entry<String, List<Event>> entry : data.entrySet()) {
                dos.writeUTF(entry.getKey()); // 2. Escreve Nome Produto.
                List<Event> events = entry.getValue();
                dos.writeInt(events.size()); // 3. Escreve Quantidade de Eventos.
                for (Event e : events) {
                    e.write(dos); // 4. Escreve cada Evento (ver Event.java).
                }
            }
        }
    }

    // Obtém dados agregados (Soma, Média, etc).
    public AggregationCache getAggregation(int day, String product) throws IOException {
        // Se o pedido for mais antigo que o histórico permitido (D), ignora.
        if ((currentDay - day) > maxHistoryDays) {
            return new AggregationCache();
        }

        String cacheKey = day + ":" + product;
        // Se já calculámos isto antes e não é o dia de hoje, devolve da cache.
        if (day != currentDay && aggregationCache.containsKey(cacheKey)) {
            return aggregationCache.get(cacheKey);
        }

        AggregationCache result = new AggregationCache();

        boolean inMemory;
        globalLock.lock();
        try {
            inMemory = daysInMemory.containsKey(day); // Verifica se está na RAM.
        } finally {
            globalLock.unlock();
        }

        if (inMemory) {
            // Se está na RAM, usamos o Lock de LEITURA (permite múltiplos leitores simultâneos).
            ReentrantReadWriteLock lock = getDayLock(day);
            lock.readLock().lock();
            try {
                Map<String, List<Event>> dayData = daysInMemory.get(day);
                if (dayData != null && dayData.containsKey(product)) {
                    for (Event e : dayData.get(product)) {
                        accumulate(result, e); // Soma os valores.
                    }
                }
            } finally {
                lock.readLock().unlock();
            }
        } else {
            // Se está no DISCO, chamamos a função de leitura em stream.
            result = aggregateFromDisk(day, product);
        }

        // Guarda na cache para a próxima (se não for o dia corrente, que está sempre a mudar).
        if (day != currentDay) {
            aggregationCache.put(cacheKey, result);
        }
        return result;
    }

    // Lê do disco SEM carregar tudo para a RAM (Streaming).
    private AggregationCache aggregateFromDisk(int day, String targetProduct) throws IOException {
        AggregationCache res = new AggregationCache();
        File f = new File(dataDirectory, "day_" + day + ".dat");

        if (!f.exists()) return res;

        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(f)))) {
            int numProducts = dis.readInt();

            for (int i = 0; i < numProducts; i++) {
                String prodName = dis.readUTF();
                int numEvents = dis.readInt();

                // Só processa se for o produto que queremos!
                if (prodName.equals(targetProduct)) {
                    for (int j = 0; j < numEvents; j++) {
                        Event e = Event.read(dis);
                        accumulate(res, e);
                    }
                } else {
                    // Se não for o produto, SALTA os dados à frente para não perder tempo.
                    for (int j = 0; j < numEvents; j++) {
                        dis.readUTF(); // Salta produto.
                        dis.readInt(); // Salta quantidade.
                        dis.readDouble(); // Salta preço.
                    }
                }
            }
        }
        return res;
    }

    // Helper para somar valores no objeto de cache.
    private void accumulate(AggregationCache res, Event e) {
        res.count += e.getQuantity(); // Soma quantidade.
        res.volume += (e.getQuantity() * e.getPrice()); // Soma volume (€).
        if (e.getPrice() > res.maxPrice) res.maxPrice = e.getPrice(); // Atualiza máximo.
    }

    //funcoes para o FILTER_EVENTS

    // Obtém o mapa completo de um dia (seja da RAM ou do Disco).
    public Map<String, List<Event>> getDayData(int day) throws IOException {
        globalLock.lock(); // Lock Global porque podemos alterar o estado da memória (carregar do disco).
        try {
            // Se já está na RAM, devolve direto.
            if (daysInMemory.containsKey(day)) return daysInMemory.get(day);

            // Se a RAM está cheia, expulsa alguém para dar lugar a este dia.
            if (daysInMemory.size() >= maxDaysInMemory) evictOneDay();

            // Carrega o dia inteiro do disco.
            Map<String, List<Event>> data = loadAllDay(day);
            // Guarda na memória (Caching).
            daysInMemory.put(day, data);
            return data;
        } finally {
            globalLock.unlock();
        }
    }

    // Lê um ficheiro .dat inteiro e reconstrói o Mapa de Eventos.
    private Map<String, List<Event>> loadAllDay(int day) throws IOException{
        Map<String, List<Event>> map = new HashMap<>();
        File f = new File(dataDirectory, "day_" + day + ".dat");
        if (!f.exists()) return map; // Se não existe ficheiro, devolve vazio.

        // Abre stream de leitura.
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(f)))) {
            int numProds = dis.readInt(); // Lê número de produtos.
            for(int i = 0; i < numProds; i++) {
                String p = dis.readUTF(); // Lê nome.
                int n = dis.readInt(); // Lê quantidade de eventos.
                List<Event> list = new ArrayList<>(n);
                // Lê cada evento individualmente.
                for(int j = 0; j<n; j++) list.add(Event.read(dis));
                map.put(p, list);
            }
        }
        return map;
    }

    // Obtém (ou cria) o Lock RW para um dia específico.
    public ReentrantReadWriteLock getDayLock(int day) {
        // computeIfAbsent: Se não existir lock para o dia 'day', cria um novo e guarda-o.
        // Isto é thread-safe porque 'dayLocks' é um ConcurrentHashMap.
        return dayLocks.computeIfAbsent(day, k -> new ReentrantReadWriteLock());
    }

    // Devolve o dia atual.
    public int getCurrentDay() {
        return currentDay;
    }
}