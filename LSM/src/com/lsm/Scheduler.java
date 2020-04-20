package com.lsm;

import java.util.List;

public class Scheduler {

    static Memory memory;

    public Scheduler(Memory memory) {
        this.memory = memory;
    }


//    TODO: Modules you need to implement:
//    TODO: 1. Use "Strict Two-Phase Locking protocol" to ensure serializability
//    TODO: 2. Deadlock detection (using "wait-for graphs") and its recovery mechanisms (using "undo recovery strategy")


    public static void scheduleTransaction(List transactions){
        System.out.println("transactions :");
        for (Object t: transactions){
            System.out.println(t.toString());
        }
//        In case of transactions, the isolation level should be Serializable

//        input format: [[transactionName(String), transaction(String)]]
//        e.g.: transactions: [  [1, "R, X, 11"], [1, "W, X, (1, Thalia, 412-656-2212)" ], [2, "R, X, 12"], [1, "C"]  ]

        /**
        The usage of memory: for more detail, refer to Main.java and Memory.java
        action  "W" :
            memory.write(table, data);
        action  "E" :
            memory.erase(table, data);
        action  "D" :
            memory.delete(table);
        action  "R" :
            memory.readID(table, data);
        action  "M":
            memory.readAreaCode(table, data);

         **/
    }

    public static void scheduleProcess(List processes){
        System.out.println("processes :");
        for (Object t: processes){
            System.out.println(t.toString());
        }
//        In case of processes, the isolation level should be Read Committed
//        The input format is the same as transactions
    }


}
