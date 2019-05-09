package com.cubestacklabs.problems.coding;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class TransactionProcessing {

    public static void main(String[] args) {
        List<Transaction> transactions = TransactionGenerator.generate();
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        Map<Integer, Map<String, Integer>> transactionHolder = new ConcurrentHashMap<>();
        Map<String, Integer> rejectTxnTickerCache = new ConcurrentHashMap<>();

        CountDownLatch latch = new CountDownLatch(transactions.size());

        transactions.forEach(txn -> executorService.submit(
                () -> processTransaction(txn, transactionHolder, rejectTxnTickerCache, latch)
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            System.out.println("Processed Txn: " + transactionHolder);
            System.out.println("rejectTxnTickerCache: " + rejectTxnTickerCache);
            executorService.shutdown();
        }
    }

    static void processTransaction(
            Transaction txn,
            Map<Integer, Map<String, Integer>> transactionHolder,
            Map<String, Integer> rejectTxnTickerCache,
            CountDownLatch latch) {
        System.out.printf("%s(txn-%s:v%s): processing txn - %s", Thread.currentThread().getName(), txn.id, txn.version, txn);
        System.out.println();
        String tickerKey = txn.tickerKey();
        if(txn.reject) {
            if(transactionHolder.containsKey(txn.id)) {
                transactionHolder.get(txn.id).remove(txn.ticker);
            } else {
                rejectTxnTickerCache.put(tickerKey, txn.version);
            }
        } else if(!rejectTxnTickerCache.containsKey(tickerKey) || rejectTxnTickerCache.get(tickerKey) < txn.version) {
            // update transactions
            Map<String, Integer> tickerQty;
            if(transactionHolder.containsKey(txn.id)) {
                tickerQty = transactionHolder.get(txn.id);
            } else {
                tickerQty = new ConcurrentHashMap<>();
                transactionHolder.put(txn.id, tickerQty);
            }
            tickerQty.put(txn.ticker, tickerQty.getOrDefault(txn.ticker, 0) + txn.qty);
        }
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            latch.countDown();
        }
    }

    static class TransactionGenerator {

        public static List<Transaction> generate() {
            List<Transaction> transactions = new ArrayList<>();

            transactions.add(new Transaction(1, 2, "IBM", -10, false));
            transactions.add(new Transaction(1, 3, "TCS", 20, false));
            transactions.add(new Transaction(1, 6, "JPM", -25, false));
            transactions.add(new Transaction(1, 1, "ABC", 50, false));
            transactions.add(new Transaction(1, 4, "JPM", 65, false));
            transactions.add(new Transaction(1, 5, "IBM", 45, false));
            transactions.add(new Transaction(1, 7, "ABC", 0, true));

            Collections.shuffle(transactions);

            return transactions;
        }
    }

    static class Transaction {
        private int id;
        private int version;
        private String ticker;
        private int qty;
        private boolean reject;

        public Transaction(int id, int version, String ticker, int qty, boolean reject) {
            this.id = id;
            this.version = version;
            this.ticker = ticker;
            this.qty = qty;
            this.reject = reject;
        }

        public String tickerKey() {
            return String.format("txn-%s:%s", id, ticker);
        }

        @Override
        public String toString() {
            return "Transaction[" +
                    "id=" + id +
                    ", version=" + version +
                    ", ticker=" + ticker +
                    ", qty=" + qty +
                    ", reject=" + reject +
                    ']';
        }
    }
}
