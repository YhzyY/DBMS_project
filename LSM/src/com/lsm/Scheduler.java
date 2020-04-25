

package com.lsm;

import java.util.*;

public class Scheduler {

    public static final int TOT_OPERATIONS = 5;
    public static final int TOT_VERTICES = 10000;
    public static ArrayList<ArrayList<Integer>>wait_for_graphT = new ArrayList<ArrayList<Integer>>(TOT_VERTICES);


    static Memory memory;

    public Scheduler(Memory memory) {
        this.memory = memory;
    }


//    TODO: Modules you need to implement:
//    TODO: 1. Use "Strict Two-Phase Locking protocol" to ensure serializability
//    TODO: 2. Deadlock detection (using "wait-for graphs") and its recovery mechanisms (using "undo recovery strategy")

    /*********************************************************
                    Scheduling on transactions
    **************************************************************/


    public static void add_edge(Integer source,Integer dest)
    {
        System.out.println("I am adding and edge from " + source +" to " + dest);
        if(!wait_for_graphT.get(source).contains(dest))
            wait_for_graphT.get(source).add(dest);
    }

    public static void init_graph()
    {
        wait_for_graphT = new ArrayList<ArrayList<Integer>>(TOT_VERTICES);

        for(int i=0;i<TOT_VERTICES;i++)
            wait_for_graphT.add(new ArrayList<Integer>());

        //System.out.println("Size of the wait for graph " + wait_for_graphT.size());
    }
    public static boolean isCycle(int source,boolean[] visited,boolean[] recStack)
    {

        //Iterate through neighbours and check for cycle
        //1 -> 2 -> (3 ->5) ->(3-->2)

        //System.out.println("THE CURRENT SOURCE "+source);
        if(visited[source])
        {
            recStack[source] = false;
            return false;
        }


        visited[source] = true;
        recStack[source] = true;

        for(Integer neighbour: wait_for_graphT.get(source))
        {
            //It means that it is in the recursive stack(so there is a back edge)
            //so cycle exists
            //System.out.println("Neighbour " + neighbour);
            //System.out.println("Neighbours " + neighbour + " visited " + visited[neighbour] + " recStack " + recStack[neighbour]);
            if(!visited[neighbour] && isCycle(neighbour,visited,recStack))
                return true;
            else if(recStack[neighbour])
                return true;
        }

        recStack[source] = false;
        //At this point the vertices and related neighbours do not form a cycle
        return false;
    }



    public static void scheduleTransaction(List transactions){
        //System.out.println("transactions :");

       // System.out.println(transactions.get(0).toString().charAt(1));

        //This will have a map from Key(tuple ID)
        //t.toString().charAt(1) will

        /***********************************************************
         [1]Construct lock table(keep track of request to a data item sequence so that during commit we need to reassign resources)
         [2]Differentiate read lock and write lock(Share read locks -> no conflicts here)
         [3]Do all the writes to disk during commit time
         ***********************************************************/


        init_graph();
        //System.out.println("The size of the graph is " + wait_for_graphT.size());

        boolean isFinalCycle = false;

        //Another hashmap where key is the transaction ID and value is the list of resources it is holding
        HashMap<String, ArrayList<String>>txn_resources = new HashMap<String,ArrayList<String>>();

        //Lock table is hashmap of resources waiting for access when a transaction holding that resource commits
        HashMap<String,ArrayList<String>>lock_table = new HashMap<String,ArrayList<String>>();


        //Hashmap key as resource and the value can {RL,WL}
        HashMap<String,ArrayList<Integer>>isResourceFree = new HashMap<String,ArrayList<Integer>>();
        HashMap<String,ArrayList<String>>resource_lock = new HashMap<String,ArrayList<String>>();


        ArrayList<Integer>txn_ids = new ArrayList<Integer>();
        int no_vertices = 0;


        //Construct the graph by the time the loop ends
        System.out.println("_________________History______________");
        for(Object t:transactions)
        {
            System.out.println(t);
        }
        System.out.println("_______________________________________");

        for (Object t: transactions){
            String transaction_id = t.toString().split(",")[0].substring(1);

            if(!txn_ids.contains(Integer.valueOf(transaction_id)))
            {
                txn_ids.add(Integer.valueOf(transaction_id));
                no_vertices++;
            }

            String operation = t.toString().substring(3).trim();
            operation = operation.substring(0,operation.length()-1);

            //System.out.println("____________CUR OPERATION____________");
            System.out.println(operation);
            //System.out.println("_____________________________________");



            if(operation.charAt(0) == 'R')
            {
                //System.out.println("________________");
                //System.out.println("Read on ID from "+transaction_id);
                //System.out.println(operation);
                //System.out.println("_________________");

                //operation_meta[0] has the operation name
                //operation[1] has the table name
                //operartion[2] has the ID

                //Key will be tablename_ID
                String[] operation_meta = operation.split(" ");

                //NEED TO CHECK IF THERE IS TABLE LEVEL LOCK FIRST AND THEN CHECK IF THERE IS A TUPLE LEVEL LOCK
                //(MULTI GRANULARITY LOCKING)

                //Key: "operation_meta[1]+"_"+operation_meta[2]

                //Check first if there is a table level then check if there is tuple level lock
                //System.out.println(operation_meta[1]+"_"+operation_meta[2]+"->"+transaction_id);
                //System.out.println(isResourceFree.containsKey(operation_meta[1]+"_"+operation_meta[2]));

                //Check if the resource current has a lock, then check if that resource has a write lock, and that write
                //lock does not belong to this transaction
                if( ( isResourceFree.containsKey(operation_meta[1]) )
                        && ( resource_lock.get(operation_meta[1]).contains("WL") )
                            && ( !isResourceFree.get(operation_meta[1]).get(0).equals(Integer.valueOf(transaction_id))  ) )
                {
                    String key = operation_meta[1];
                    if(lock_table.containsKey(key))
                    {
                        lock_table.get(key).add(transaction_id+"_RL");
                    }
                    else
                    {
                        ArrayList<String>temp = new ArrayList<String>();
                        temp.add(transaction_id+"_RL");
                        lock_table.put(key,temp);
                    }

                    ArrayList<Integer>all_dest = new ArrayList<Integer>();
                    all_dest = isResourceFree.get(operation_meta[1]);

                    for(Integer dest : all_dest)
                    {
                        if(!dest.equals(Integer.valueOf(transaction_id)))
                        {
                            Scheduler.add_edge(Integer.valueOf(transaction_id),dest);
                        }
                        else
                        {
                            //System.out.println("SOMETHING UNEXPECTED HAPPENED EDGE REFERING TO ITSELF");
                        }
                    }

                    //Scheduler.add_edge(Integer.valueOf(transaction_id),isResourceFree.get(operation_meta[1]));
                }
                else if(isResourceFree.containsKey(operation_meta[1]+"_"+operation_meta[2])
                        && resource_lock.get(operation_meta[1]+"_"+operation_meta[2]).contains("WL")
                            && ( !isResourceFree.get(operation_meta[1]+"_"+operation_meta[2]).get(0).equals(Integer.valueOf(transaction_id))  ))
                {
                    //Then someone else is holding the resource
                    String key = operation_meta[1]+"_"+operation_meta[2];
                    if(lock_table.containsKey(key))
                    {
                        lock_table.get(key).add(transaction_id+"_RL");
                    }
                    else
                    {
                        ArrayList<String>temp = new ArrayList<String>();
                        temp.add(transaction_id+"_RL");
                        lock_table.put(key,temp);
                    }

                    ArrayList<Integer>all_dest = new ArrayList<Integer>();
                    all_dest = isResourceFree.get(operation_meta[1]+"_"+operation_meta[2]);

                    for(Integer dest : all_dest)
                    {
                        if(!dest.equals(Integer.valueOf(transaction_id)))
                        {
                           Scheduler.add_edge(Integer.valueOf(transaction_id),dest);
                        }
                        else
                        {
                            //System.out.println("SOMETHING UNEXPECTED HAPPENED EDGE REFERING TO ITSELF");
                        }
                    }

                    /*
                    for(Integer dest: isResourceFree.get(operation_meta[1]+"_"+operation_meta[2]))
                    {

                    }
                   Scheduler.add_edge(Integer.valueOf(transaction_id),isResourceFree.get(operation_meta[1]+"_"+operation_meta[2]));

                     */

                }
                else
                {
                    //System.out.println(operation_meta[1]+"_"+operation_meta[2]+" is a new resource");

                    //System.out.println(isResourceFree.get(operation_meta[1]+"_"+operation_meta[2]));
                    //System.out.println(isResourceFree.get(operation_meta[1]+"_"+operation_meta[2]).indexOf(Integer.valueOf(transaction_id)));
                    //System.out.println(resource_lock.get(operation_meta[1]+"_"+operation_meta[2]).get(isResourceFree.get(operation_meta[1]+"_"+operation_meta[2]).indexOf(Integer.valueOf(transaction_id))));
                    //System.out.println(resource_lock.get(operation_meta[1]+"_"+operation_meta[2]).get(isResourceFree.get(operation_meta[1]+"_"+operation_meta[2]).indexOf(Integer.valueOf(transaction_id))).equals("RL"));
                    //Add multiple instances of read lock

                    //System.out.println();

                    if(isResourceFree.containsKey(operation_meta[1]+"_"+operation_meta[2])
                            && isResourceFree.get(operation_meta[1]+"_"+operation_meta[2]).contains(Integer.valueOf(transaction_id))
                                && resource_lock.get(operation_meta[1]+"_"+operation_meta[2]).get(isResourceFree.get(operation_meta[1]+"_"+operation_meta[2]).indexOf(Integer.valueOf(transaction_id))).equals("RL"))
                    {
                        //Redundancy
                    }
                    else
                    {
                        if(isResourceFree.containsKey(operation_meta[1]+"_"+operation_meta[2]))
                        {
                            isResourceFree.get(operation_meta[1]+"_"+operation_meta[2]).add(Integer.valueOf(transaction_id));
                            resource_lock.get(operation_meta[1]+"_"+operation_meta[2]).add("RL");
                        }
                        else
                        {
                            ArrayList<Integer>temp = new ArrayList<Integer>();
                            temp.add(Integer.valueOf(transaction_id));
                            isResourceFree.put(operation_meta[1]+"_"+operation_meta[2],temp);

                            ArrayList<String>temp1 = new ArrayList<String>();
                            temp1.add("RL");
                            resource_lock.put(operation_meta[1]+"_"+operation_meta[2],temp1);
                        }
                    }

                    //isResourceFree.put(operation_meta[1]+"_"+operation_meta[2],Integer.valueOf(transaction_id));
                    //resource_lock.put(operation_meta[1]+"_"+operation_meta[2],"RL");

                    if(txn_resources.containsKey(transaction_id))
                    {
                        txn_resources.get(transaction_id).add(operation_meta[1]+"_"+operation_meta[2]);
                    }
                    else
                    {
                        ArrayList<String>temp = new ArrayList<String>();
                        temp.add(operation_meta[1]+"_"+operation_meta[2]);
                        txn_resources.put(transaction_id,temp);
                    }
                }
            }
            else if(operation.charAt(0) == 'M')
            {
                //operation_meta[0] has the operation name
                //operation[1] has the table name
                //operartion[2] has the area

                //Key will be tablename_ID
                String[] operation_meta = operation.split(" ");

                //We need the tuples with the matching area code
                //System.out.println("________________");
                //System.out.println("Read on Area code");
                //System.out.println(operation);
                //System.out.println("_________________");
                ArrayList<Integer>id_list = new ArrayList<Integer>();
                id_list = Memory.returnAreaCodes(operation_meta[1],operation_meta[2]);

                //place locks on those id list
                for(int i=0;i<id_list.size();i++)
                {
                    if(isResourceFree.containsKey(operation_meta[1])
                            && resource_lock.get(operation_meta[1]).contains("WL")
                                && ( isResourceFree.get(operation_meta[1]).get(0).equals(Integer.valueOf(transaction_id)) ) )
                    {
                        String key = operation_meta[1];
                        if(lock_table.containsKey(key))
                        {
                            lock_table.get(key).add(transaction_id+"_RL");
                        }
                        else
                        {
                            ArrayList<String>temp = new ArrayList<String>();
                            temp.add(transaction_id+"_RL");
                            lock_table.put(key,temp);
                        }
                        ArrayList<Integer>all_dest = new ArrayList<Integer>();
                        all_dest = isResourceFree.get(operation_meta[1]);

                        for(Integer dest : all_dest)
                        {
                            if(!dest.equals(Integer.valueOf(transaction_id)))
                            {
                                Scheduler.add_edge(Integer.valueOf(transaction_id),dest);
                            }
                            else
                            {
                                //System.out.println("SOMETHING UNEXPECTED HAPPENED EDGE REFERING TO ITSELF");
                            }
                        }
                        //Scheduler.add_edge(Integer.valueOf(transaction_id),isResourceFree.get(operation_meta[1]));
                    }
                    else if(isResourceFree.containsKey(operation_meta[1]+"_"+id_list.get(i))
                            && resource_lock.get(operation_meta[1]+"_"+id_list.get(i)).contains("WL")
                                && ( !isResourceFree.get(operation_meta[1]+"_"+id_list.get(i)).get(0).equals(Integer.valueOf(transaction_id)) ))
                    {
                        //Then someone else is holding the resource
                        String key = operation_meta[1]+"_"+id_list.get(i);
                        if(lock_table.containsKey(key))
                        {
                            lock_table.get(key).add(transaction_id+"_RL");
                        }
                        else
                        {
                            ArrayList<String>temp = new ArrayList<String>();
                            temp.add(transaction_id+"_RL");
                            lock_table.put(key,temp);
                        }

                        ArrayList<Integer>all_dest = new ArrayList<Integer>();
                        all_dest = isResourceFree.get(operation_meta[1]+"_"+id_list.get(i));

                        for(Integer dest : all_dest)
                        {
                            if(!dest.equals(Integer.valueOf(transaction_id)))
                            {
                                Scheduler.add_edge(Integer.valueOf(transaction_id),dest);
                            }
                            else
                            {
                                //System.out.println("SOMETHING UNEXPECTED HAPPENED EDGE REFERING TO ITSELF");
                            }
                        }

                        //Scheduler.add_edge(Integer.valueOf(transaction_id),isResourceFree.get(operation_meta[1]+"_"+id_list.get(i)));

                    }
                    else
                    {


                        if(isResourceFree.containsKey(operation_meta[1]+"_"+id_list.get(i))
                                && isResourceFree.get(operation_meta[1]+"_"+id_list.get(i)).contains(Integer.valueOf(transaction_id)))
                        {
                            //Redundancy
                        }
                        else if(isResourceFree.containsKey(operation_meta[1]+"_"+id_list.get(i)))
                        {
                            isResourceFree.get(operation_meta[1]+"_"+id_list.get(i)).add(Integer.valueOf(transaction_id));
                            resource_lock.get(operation_meta[1]+"_"+id_list.get(i)).add("RL");
                        }
                        else
                        {
                            ArrayList<Integer>temp = new ArrayList<Integer>();
                            temp.add(Integer.valueOf(transaction_id));
                            isResourceFree.put(operation_meta[1]+"_"+id_list.get(i),temp);

                            ArrayList<String>temp1 = new ArrayList<String>();
                            temp1.add("RL");
                            resource_lock.put(operation_meta[1]+"_"+id_list.get(i),temp1);
                        }

                        //isResourceFree.put(operation_meta[1]+"_"+id_list.get(i),Integer.valueOf(transaction_id));
                        //resource_lock.put(operation_meta[1]+"_"+id_list.get(i),"RL");

                        if(txn_resources.containsKey(transaction_id))
                        {
                            txn_resources.get(transaction_id).add(operation_meta[1]+"_"+id_list.get(i));
                        }
                        else
                        {
                            ArrayList<String>temp = new ArrayList<String>();
                            temp.add(operation_meta[1]+"_"+id_list.get(i));
                            txn_resources.put(transaction_id,temp);
                        }
                    }
                }
            }

            else if(operation.charAt(0) == 'W')
            {
                //System.out.println(operation);
                String table_name = operation.split(" ")[1];
                String id = operation.substring(5,operation.length()-1).split(",")[0];

                //System.out.println("GOING TO FORM THE KEY "+table_name+"_"+id);

                //I should first check if that tuple is free or not(since this could be an update operation also)

                //Check if there is a lock present, if that lock is present check if there is only one lock on
                //that data item and that is a read done by the same transaction
                if(isResourceFree.containsKey(table_name) &&
                        (isResourceFree.get(table_name).size() > 1
                                || !isResourceFree.get(table_name).get(0).equals(Integer.valueOf(transaction_id))))
                {
                    String key = table_name;
                    if(lock_table.containsKey(key))
                    {
                        lock_table.get(key).add(transaction_id+"_WL");
                    }
                    else
                    {
                        ArrayList<String>temp = new ArrayList<String>();
                        temp.add(transaction_id+"_WL");
                        lock_table.put(key,temp);
                    }
                    ArrayList<Integer>all_dest = new ArrayList<Integer>();
                    all_dest = isResourceFree.get(table_name);

                    for(Integer dest : all_dest)
                    {
                        if(!dest.equals(Integer.valueOf(transaction_id)))
                        {
                            Scheduler.add_edge(Integer.valueOf(transaction_id),dest);
                        }
                        else
                        {
                            //System.out.println("SOMETHING UNEXPECTED HAPPENED EDGE REFERING TO ITSELF");
                        }
                    }
                    //Scheduler.add_edge(Integer.valueOf(transaction_id),isResourceFree.get(table_name));
                }
                else if(isResourceFree.containsKey(table_name+"_"+id) &&
                (isResourceFree.get(table_name+"_"+id).size() > 1
                        || !isResourceFree.get(table_name+"_"+id).get(0).equals(Integer.valueOf(transaction_id))) )
                {
                    String key = table_name+"_"+id;
                    if(lock_table.containsKey(key))
                    {
                        lock_table.get(key).add(transaction_id+"_WL");
                    }
                    else
                    {
                        ArrayList<String>temp = new ArrayList<String>();
                        temp.add(transaction_id+"_WL");
                        lock_table.put(key,temp);
                    }

                    ArrayList<Integer>all_dest = new ArrayList<Integer>();
                    all_dest = isResourceFree.get(table_name+"_"+id);

                    for(Integer dest : all_dest)
                    {
                        if(!dest.equals(Integer.valueOf(transaction_id)))
                        {
                            Scheduler.add_edge(Integer.valueOf(transaction_id),dest);
                        }
                        else
                        {
                            //System.out.println("SOMETHING UNEXPECTED HAPPENED EDGE REFERING TO ITSELF");
                        }
                    }
                    //Scheduler.add_edge(Integer.valueOf(transaction_id),isResourceFree.get(table_name+"_"+id));
                }
                else
                {
                    //Need to check for redudant write locks
                    boolean isRedundant = false;

                    if(isResourceFree.containsKey(table_name+"_"+id))
                    {
                        int i = 0;
                        for(Integer Txn_id: isResourceFree.get(table_name+"_"+id))
                        {
                            if(Txn_id.equals(Integer.valueOf(transaction_id))
                                    && (resource_lock.get(table_name+"_"+id).get(i).equals("WL")))
                            {
                                isRedundant = true;
                                break;
                            }
                        }
                    }

                    if(isRedundant)
                    {
                        //DO nothing
                    }
                    else if(isResourceFree.containsKey(table_name+"_"+id))
                    {
                        isResourceFree.get(table_name+"_"+id).add(Integer.valueOf(transaction_id));
                        resource_lock.get(table_name+"_"+id).add("WL");
                    }
                    else
                    {
                        ArrayList<Integer>temp = new ArrayList<Integer>();
                        temp.add(Integer.valueOf(transaction_id));
                        isResourceFree.put(table_name+"_"+id,temp);

                        ArrayList<String>temp1 = new ArrayList<String>();
                        temp1.add("WL");
                        resource_lock.put(table_name+"_"+id,temp1);
                    }


                    //isResourceFree.put(table_name+"_"+id,Integer.valueOf(transaction_id));
                    //resource_lock.put(table_name+"_"+id,"WL");

                    if(!isRedundant)
                    {
                        if(txn_resources.containsKey(transaction_id))
                        {
                            txn_resources.get(transaction_id).add(table_name+"_"+id);
                        }
                        else
                        {
                            ArrayList<String>temp = new ArrayList<String>();
                            temp.add(table_name+"_"+id);
                            txn_resources.put(transaction_id,temp);
                        }
                    }

                }


            }

            else if(operation.charAt(0) == 'E')
            {
                /*
                System.out.println("________________");
                System.out.println("Delete on ID");
                System.out.println(operation);
                System.out.println("_________________");

                 */
                String table_name = operation.split(" ")[1];
                String id = operation.split(" ")[2];

                //System.out.println("Table name "+table_name);
                //System.out.println("Id "+id);

                String[] operation_meta = {table_name,id};

                if(isResourceFree.containsKey(operation_meta[0])
                     &&   (isResourceFree.get(operation_meta[0]).size() > 1
                                || !isResourceFree.get(operation_meta[0]).get(0).equals(Integer.valueOf(transaction_id))))
                {
                    String key = operation_meta[0];
                    if(lock_table.containsKey(key))
                    {
                        lock_table.get(key).add(transaction_id+"_WL");
                    }
                    else
                    {
                        ArrayList<String>temp = new ArrayList<String>();
                        temp.add(transaction_id+"_WL");
                        lock_table.put(key,temp);
                    }

                    ArrayList<Integer>all_dest = new ArrayList<Integer>();
                    all_dest = isResourceFree.get(operation_meta[0]);

                    for(Integer dest : all_dest)
                    {
                        if(!dest.equals(Integer.valueOf(transaction_id)))
                        {
                            Scheduler.add_edge(Integer.valueOf(transaction_id),dest);
                        }
                        else
                        {
                            //System.out.println("SOMETHING UNEXPECTED HAPPENED EDGE REFERING TO ITSELF");
                        }
                    }

                    //Scheduler.add_edge(Integer.valueOf(transaction_id),isResourceFree.get(operation_meta[0]));
                }
                else if(isResourceFree.containsKey(operation_meta[0]+"_"+operation_meta[1]) &&
                        (isResourceFree.get(operation_meta[0]+"_"+operation_meta[1]).size() > 1
                                || !isResourceFree.get(operation_meta[0]+"_"+operation_meta[1]).get(0).equals(Integer.valueOf(transaction_id))))
                {
                    String key = operation_meta[0]+"_"+operation_meta[1];
                    if(lock_table.containsKey(key))
                    {
                        lock_table.get(key).add(transaction_id+"_WL");
                    }
                    else
                    {
                        ArrayList<String>temp = new ArrayList<String>();
                        temp.add(transaction_id+"_WL");
                        lock_table.put(key,temp);
                    }

                    ArrayList<Integer>all_dest = new ArrayList<Integer>();
                    all_dest = isResourceFree.get(key);

                    for(Integer dest : all_dest)
                    {
                        if(!dest.equals(Integer.valueOf(transaction_id)))
                        {
                            Scheduler.add_edge(Integer.valueOf(transaction_id),dest);
                        }
                        else
                        {
                            //System.out.println("SOMETHING UNEXPECTED HAPPENED EDGE REFERING TO ITSELF");
                        }
                    }
                    //Scheduler.add_edge(Integer.valueOf(transaction_id),isResourceFree.get(operation_meta[0]+"_"+operation_meta[1]));
                }
                else
                {
                    //isResourceFree.put(operation_meta[0]+"_"+operation_meta[1],Integer.valueOf(transaction_id));
                    //resource_lock.put(operation_meta[0]+"_"+operation_meta[1],"WL");



                    if(isResourceFree.containsKey(operation_meta[0]+"_"+operation_meta[1]))
                    {
                        isResourceFree.get(operation_meta[0]+"_"+operation_meta[1]).add(Integer.valueOf(transaction_id));
                        resource_lock.get(operation_meta[0]+"_"+operation_meta[1]).add("WL");
                    }
                    else
                    {
                        ArrayList<Integer>temp = new ArrayList<Integer>();
                        temp.add(Integer.valueOf(transaction_id));
                        isResourceFree.put(operation_meta[0]+"_"+operation_meta[1],temp);

                        ArrayList<String>temp1 = new ArrayList<String>();
                        temp1.add("WL");
                        resource_lock.put(operation_meta[0]+"_"+operation_meta[1],temp1);
                    }

                    if(txn_resources.containsKey(transaction_id))
                    {
                        txn_resources.get(transaction_id).add(operation_meta[0]+"_"+operation_meta[1]);
                    }
                    else
                    {
                        ArrayList<String>temp = new ArrayList<String>();
                        temp.add(operation_meta[0]+"_"+operation_meta[1]);
                        txn_resources.put(transaction_id,temp);
                    }
                }
            }
            else if(operation.charAt(0) == 'D')
            {
                //place a lock on the table if free
                //operation_meta[0] contains the operation
                //operation_meta[1] contains the table to delete
                String table_name = operation.split(" ")[1];

                String[] operation_meta = {"",table_name};
                boolean isConflict = false;

                //Place a lock on the table itself
                Iterator hmIterator = isResourceFree.entrySet().iterator();
                while (hmIterator.hasNext())
                {
                    Map.Entry mapElement = (Map.Entry)hmIterator.next();
                    String key = (String)mapElement.getKey();

                    //If there is tuple level lock on that table and if there is a table lock
                    if(operation_meta[1].equals(key.split("_")[0]) || operation_meta[1].equals(key))
                    {

                        //W Y_12, D Y  RL_Y_12 = <TXN=T1>

                        if(lock_table.containsKey(key))
                        {
                            lock_table.get(key).add(transaction_id+"_WL");
                        }
                        else
                        {
                            ArrayList<String>temp = new ArrayList<String>();
                            temp.add(transaction_id+"_WL");
                            lock_table.put(key,temp);
                        }

                        ArrayList<Integer>all_dest = new ArrayList<Integer>();
                        all_dest = isResourceFree.get(key);

                        for(Integer dest : all_dest)
                        {
                            if(!dest.equals(Integer.valueOf(transaction_id)))
                            {
                                Scheduler.add_edge(Integer.valueOf(transaction_id),dest);
                            }
                            else
                            {
                                //System.out.println("SOMETHING UNEXPECTED HAPPENED EDGE REFERING TO ITSELF");
                            }
                        }

                        //add_edge(Integer.valueOf(transaction_id),isResourceFree.get(key));
                        isConflict = true;
                    }

                }


                if(!isConflict)
                {


                    //isResourceFree.put(operation_meta[1],Integer.valueOf(transaction_id));
                    //resource_lock.put(operation_meta[1],"WL");

                    if(isResourceFree.containsKey(operation_meta[1]))
                    {
                        isResourceFree.get(operation_meta[1]).add(Integer.valueOf(transaction_id));
                        resource_lock.get(operation_meta[1]).add("WL");
                    }
                    else
                    {
                        ArrayList<Integer>temp = new ArrayList<Integer>();
                        temp.add(Integer.valueOf(transaction_id));
                        isResourceFree.put(operation_meta[1],temp);

                        ArrayList<String>temp1 = new ArrayList<String>();
                        temp1.add("WL");
                        resource_lock.put(operation_meta[1],temp1);
                    }

                    //Add to transactions resources
                    if(txn_resources.containsKey(transaction_id))
                    {
                        txn_resources.get(transaction_id).add(operation_meta[1]);
                    }
                    else
                    {
                        ArrayList<String>temp = new ArrayList<String>();
                        temp.add(operation_meta[1]);
                        txn_resources.put(transaction_id,temp);
                    }
                }


            }
            else if(operation.charAt(0) == 'C' || operation.charAt(0) == 'A')
            {
                //It is better to check for commit and then abort

                //You need to free all the resources that have been locked by the corresponding
                //vertices and remove the vertices in the graph accordingly

                Integer remove_id = Integer.valueOf(transaction_id);


                //Remove all the incoming and outgoing edges related to this remove_id
                wait_for_graphT.get(Integer.valueOf(transaction_id)).clear();

                //Other transaction dependency on this transaction
                for(int i=0;i<=no_vertices+1;i++) {
                    if (wait_for_graphT.get(i).contains(remove_id))
                        wait_for_graphT.get(i).remove(remove_id);
                }

                //System.out.println("Remove id is_"+remove_id);
                //System.out.println("Value of remove id " + txn_resources.get(transaction_id));

                //Find the next transaction that wants this resource and add edges accordingly
                for(String resource: txn_resources.get(String.valueOf(transaction_id)))
                {


                    Integer next_transaction = null;
                    boolean isRead = true;

                   //System.out.println("Resource " + resource);
                    if(lock_table.containsKey(resource) && lock_table.get(resource).size() != 0)
                    {
                        next_transaction = Integer.valueOf(lock_table.get(resource).get(0).split("_")[0]);

                        if(lock_table.get(resource).get(0).split("_")[1].equals("WL"))
                        {
                            isRead = false;
                        }
                    }

                    //I need to remove the transaction id from that resource

                    int remove_index = isResourceFree.get(resource).indexOf(remove_id);
                    //System.out.println("REMOVE ID IS " + remove_id);
                    ArrayList<Integer>temp_isResource = new ArrayList<Integer>();
                    ArrayList<String>temp_lock = new ArrayList<String>();

                    int i = 0;
                    for(Integer txns: isResourceFree.get(resource))
                    {
                        if(!txns.equals(remove_id))
                        {
                            temp_isResource.add(txns);
                            temp_lock.add(resource_lock.get(resource).get(i));
                        }
                        i++;
                    }

                    isResourceFree.put(resource,temp_isResource);
                    resource_lock.put(resource,temp_lock);
                    //isResourceFree.get(resource).remove(remove_index);
                    //resource_lock.get(resource).remove(remove_index);

                    if(isResourceFree.get(resource).size() == 0)
                    {
                        isResourceFree.remove(resource);
                        resource_lock.remove(resource);
                    }

                    //Avoids self edge
                    if(next_transaction == null | transaction_id.equals(String.valueOf(next_transaction))) continue;

                    if(isResourceFree.containsKey(resource))
                    {
                        isResourceFree.get(resource).add(Integer.valueOf(next_transaction));
                        resource_lock.get(resource).add(isRead?"RL":"WL");
                    }
                    else
                    {
                        ArrayList<Integer>temp = new ArrayList<Integer>();
                        temp.add(Integer.valueOf(next_transaction));
                        isResourceFree.put(resource,temp);

                        ArrayList<String>temp1 = new ArrayList<String>();
                        temp1.add(isRead?"RL":"WL");
                        resource_lock.put(resource,temp1);
                    }


                    //Remove the first element from the lock_table

                    //THIS MIGHT SUFFER CONCURRENT MODIFICATION EXCEPTION
                    //System.out.println("THE NEXT TRANSACTION IS " + next_transaction);
                    System.out.println(transaction_id+" "+next_transaction);

                    if(txn_resources.containsKey(String.valueOf(next_transaction)))
                        txn_resources.get(String.valueOf(next_transaction)).add(resource);
                    else
                    {
                        ArrayList<String>temp = new ArrayList<String>();
                        temp.add(resource);
                        txn_resources.put(String.valueOf(next_transaction),temp);
                    }


                    lock_table.get(resource).remove(0);

                    if(lock_table.get(resource).size() == 0)
                        lock_table.remove(resource);
                    else
                    {
                        for(String txns: lock_table.get(resource))
                        {
                            if(!Integer.valueOf(txns.split("_")[0]).equals(next_transaction))
                                add_edge(Integer.valueOf(txns.split("_")[0]),next_transaction);
                        }
                    }

                }

                //Remove Everything related to that transaction
                txn_resources.remove(transaction_id);
                txn_ids.remove(remove_id);


            }
            //We will check if the graph has a cycle for every iteration

            System.out.println("_________The adjacency list for this graph_____________ "+wait_for_graphT.size());
            for(int i=0;i<=no_vertices+1;i++)
            {
                System.out.print(i+" ");
                for(Integer neighbour: wait_for_graphT.get(i))
                {
                    System.out.print(","+neighbour);
                }
                System.out.println();
            }

            //Check for graph cycle if it exists then deadlock is detected

            //System.out.println("________________Cycle detection___________________");
            boolean[] visited = new boolean[TOT_VERTICES];
            boolean[] inCurStack = new boolean[TOT_VERTICES];
            boolean is_cycle = false;
            for(int i=1;i<=no_vertices+1;i++)
            {
                //Check if the transaction is alive
                //if(txn_resources.containsKey(i))
                    //System.out.println("Transaction " + i + " is present");

                if(txn_ids.contains(i) && isCycle(i,visited,inCurStack))
                {
                    //System.out.println("Deadlock Detected");
                    is_cycle = true;
                }
            }

            if(is_cycle)
            {
                isFinalCycle = true;
                //System.out.println("Deadlock has been detected in this history");
                break;
            }
            else
            {
                //System.out.println("There is no deadlock in this history");
            }

            System.out.println("_______________________________________________________");

            System.out.println("______________TXN resources ___________");
            for(String keys: txn_resources.keySet())
            {
                System.out.println("_"+keys + "_ " + txn_resources.get(keys));
            }
            System.out.println("______________________________________");

            System.out.println("______________Lock Table ___________");
            for(String keys: lock_table.keySet())
            {
                System.out.println("_"+keys + "_ " + lock_table.get(keys));
            }
            System.out.println("______________________________________");

            System.out.println("______________isResource Table ___________");
            for(String keys: isResourceFree.keySet())
            {
                System.out.println("_"+keys + "_ " + isResourceFree.get(keys));
            }
            System.out.println("______________________________________");
            System.out.println("______________Resource lock Table ___________");
            for(String keys: resource_lock.keySet())
            {
                System.out.println("_"+keys + "_ " +resource_lock.get(keys));
            }
            System.out.println("______________________________________");


        }


        if(isFinalCycle)
        {
            System.out.println("Deadlock detected in history");
        }
        else
        {
            System.out.println("No deadlock detected in history");
        }

        /*
        System.out.println("___________Hashmap contents_____________");
        Iterator hmIterator = isResourceFree.entrySet().iterator();
        while (hmIterator.hasNext()) {
            Map.Entry mapElement = (Map.Entry)hmIterator.next();
            System.out.println(mapElement.getKey()+" "+mapElement.getValue());
        }
        System.out.println("________________________________________");

         */

        //Now check if the wait for graph has cycle in it or not.



        //Check if the wait for graph has a cycle or not

        //T1 > X_1
        //T2 > X_2
        //T1 wants X_2
        //T2 wants X_1
        //commit T1


        /*
        1. Create a hashmap where key is the tuple id and the value is the transaction id
        2. If the key(Key value is Ti) contains then lock exists on that tuple so the other transaction(Tj) has to wait
        3. If 2 is true then I will add an edge between Ti and Tj
        4.To check if they are conflicting I will check the compatibility table
        5. After iterating through the transactions check if the graph has a cycle.
         */



        /*
        for (Object t: transactions){
            System.out.println(t.toString());
        }
        */



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

    /************************************************************************************************
                                Scheduling of process starts
     ************************************************************************************************/

    //Graph related functions

    public static HashMap<Integer,ArrayList<String>>commit_operations = new HashMap<Integer,ArrayList<String>>();

    public static ArrayList<ArrayList<Integer>>wait_for_graphP = new ArrayList<ArrayList<Integer>>(TOT_VERTICES);

    public static void init_graphP()
    {
        wait_for_graphP = new ArrayList<ArrayList<Integer>>(TOT_VERTICES);

        for(int i=0;i<TOT_VERTICES;i++)
            wait_for_graphP.add(new ArrayList<Integer>());

        //System.out.println("Size of the wait for graph " + wait_for_graphT.size());
    }

    public static void add_edgeP(Integer source,Integer dest)
    {
        System.out.println("I am adding and edge from " + source +" to " + dest);
        if(!wait_for_graphP.get(source).contains(dest))
            wait_for_graphP.get(source).add(dest);
    }

    public static void removeVertex(Integer remove_id,int no_of_vertices)
    {
        wait_for_graphP.get(remove_id).clear();

        for(int i=0;i<=no_of_vertices;i++)
        {
            if(wait_for_graphP.get(i).contains(remove_id))
            {
                wait_for_graphP.get(i).remove(remove_id);
            }
        }

    }

    public static boolean isCycleP(int source,boolean[] visited,boolean[] recStack)
    {

        //Iterate through neighbours and check for cycle
        //1 -> 2 -> (3 ->5) ->(3-->2)

        //System.out.println("THE CURRENT SOURCE "+source);
        if(visited[source])
        {
            recStack[source] = false;
            return false;
        }


        visited[source] = true;
        recStack[source] = true;

        for(Integer neighbour: wait_for_graphP.get(source))
        {
            //It means that it is in the recursive stack(so there is a back edge)
            //so cycle exists
            //System.out.println("Neighbour " + neighbour);
            //System.out.println("Neighbours " + neighbour + " visited " + visited[neighbour] + " recStack " + recStack[neighbour]);
            if(!visited[neighbour] && isCycleP(neighbour,visited,recStack))
                return true;
            else if(recStack[neighbour])
                return true;
        }

        recStack[source] = false;
        //At this point the vertices and related neighbours do not form a cycle
        return false;
    }

    public static boolean checkLockFromSameProcess(String key, String transaction_id,String lock_type
            ,HashMap<String,ArrayList<Integer>>isResourceFree
            ,HashMap<String,ArrayList<String>>resource_lock)
    {
        //Currently the read,write is allowed for a transaction
        if(isResourceFree.containsKey(key))
        {
            int i = 0;
            for(Integer txn: isResourceFree.get(key))
            {
                if(txn.equals(Integer.valueOf(transaction_id))
                        && resource_lock.get(key).get(i).contains(lock_type))
                {
                    return true;
                }
                i++;
            }
        }
        return false;
    }

    public static boolean checkLockFromAnotherProcess(String key, String transaction_id,String lock_type
                                                ,HashMap<String,ArrayList<Integer>>isResourceFree
                                                    ,HashMap<String,ArrayList<String>>resource_lock)
    {
        //Currently the read,write is allowed for a transaction
        if(isResourceFree.containsKey(key))
        {
            int i = 0;
            for(Integer txn: isResourceFree.get(key))
            {
                if(!txn.equals(Integer.valueOf(transaction_id))
                        && resource_lock.get(key).get(i).contains(lock_type))
                {
                    return true;
                }
                i++;
            }
        }
        return false;
    }


    public static void updateLockTable(String transaction_id,String key
                                        ,HashMap<String,ArrayList<String>>lock_table
                                            ,HashMap<String,ArrayList<String>>resource_lock
                                                ,String lock_type)
    {
        if(lock_table.containsKey(key))
        {
            lock_table.get(key).add(transaction_id+"_"+lock_type);
        }
        else
        {
            ArrayList<String>temp = new ArrayList<String>();
            temp.add(transaction_id+"_"+lock_type);
            lock_table.put(key,temp);
        }
    }

    public static void remove_edgeP_util(Integer src,Integer dest)
    {
        //Remove edge from source to dest
        if(wait_for_graphP.get(src).size() > 0)
        {
            wait_for_graphP.get(src).remove(dest);
        }
        else
        {
            System.out.println("Lock table is not empty but edge does not exist");
        }
    }

    public static void add_edgeP_util(Integer transaction_id, String key,
                                     HashMap<String,ArrayList<Integer>>isResourceFree)
    {
        ArrayList<Integer>all_dest = new ArrayList<Integer>();
        all_dest = isResourceFree.get(key);

        for(Integer dest : all_dest)
        {
            if(!dest.equals(Integer.valueOf(transaction_id)))
            {
                System.out.println("Adding edge from "+Integer.valueOf(transaction_id)+" to "+dest);
                Scheduler.add_edgeP(Integer.valueOf(transaction_id),dest);
            }
            else
            {
                System.out.println("Unexpected Behaviour");
            }
        }

    }

    public static void updateAllMaps(boolean lockPresent,String key,String transaction_id,
                                                HashMap<String,ArrayList<Integer>>isResourceFree,
                                                    HashMap<String,ArrayList<String>>resource_lock,
                                                        HashMap<String, ArrayList<String>>txn_resources,
                                                             String lock_type)
    {
        if(!lockPresent && isResourceFree.containsKey(key))
        {
            isResourceFree.get(key).add(Integer.valueOf(transaction_id));
            resource_lock.get(key).add(lock_type);
        }
        else
        {
            ArrayList<Integer>temp = new ArrayList<Integer>();
            temp.add(Integer.valueOf(transaction_id));
            isResourceFree.put(key,temp);

            ArrayList<String>temp1 = new ArrayList<String>();
            temp1.add(lock_type);
            resource_lock.put(key,temp1);
        }

        if(!lockPresent && txn_resources.containsKey(transaction_id))
        {
            txn_resources.get(transaction_id).add(key);
        }
        else
        {
            ArrayList<String>temp = new ArrayList<String>();
            temp.add(key);
            txn_resources.put(transaction_id,temp);
        }

    }
    public static boolean checkLevelWriteLocks(String transaction_id,String table_name, String id,
                                                HashMap<String, ArrayList<String>>txn_resources,
                                                    HashMap<String,ArrayList<String>>lock_table,
                                                        HashMap<String,ArrayList<Integer>>isResourceFree,
                                                            HashMap<String,ArrayList<String>>resource_lock)
    {
        boolean addEdge = false;

        boolean lockFromAnotherProcessTable =
                checkLockFromAnotherProcess(table_name,transaction_id,"RL",isResourceFree,resource_lock)
                    && checkLockFromAnotherProcess(table_name,transaction_id,"WL",isResourceFree,resource_lock);

        boolean lockFromAotherProcessTuple =
                checkLockFromAnotherProcess(table_name+"_"+id,transaction_id,"RL",isResourceFree,resource_lock)
                    && checkLockFromAnotherProcess(table_name+"_"+id,transaction_id,"WL",isResourceFree,resource_lock);

        if(isResourceFree.containsKey(table_name) && lockFromAnotherProcessTable)
        {
            updateLockTable(transaction_id,table_name,lock_table,resource_lock,"WL");
            add_edgeP_util(Integer.valueOf(transaction_id),table_name,isResourceFree);
            addEdge = true;
        }
        else if(isResourceFree.containsKey(table_name+"_"+id) && lockFromAotherProcessTuple)
        {
            updateLockTable(transaction_id,table_name+"_"+id,lock_table,resource_lock,"WL");
            add_edgeP_util(Integer.valueOf(transaction_id),table_name+"_"+id,isResourceFree);
            addEdge = true;
        }
        else
        {
            boolean lockPresent = checkLockFromSameProcess(table_name+"_"+id,transaction_id,"WL",isResourceFree,resource_lock);
            updateAllMaps(lockPresent,table_name+"_"+id,transaction_id,isResourceFree,resource_lock,txn_resources,"WL");
        }
        return addEdge;

    }

    public static boolean checkLevelReadLocks(String transaction_id,String table_name, String id,
                                            HashMap<String, ArrayList<String>>txn_resources,
                                                HashMap<String,ArrayList<String>>lock_table,
                                                    HashMap<String,ArrayList<Integer>>isResourceFree,
                                                        HashMap<String,ArrayList<String>>resource_lock)
    {
        boolean writeFromAnotherProcessTable = false;
        boolean writeFromAnotherProcessTuple = false;
        //Check if that resource has a write lock from another transaction

        writeFromAnotherProcessTable = checkLockFromAnotherProcess(table_name,transaction_id,"WL",isResourceFree,resource_lock);
        writeFromAnotherProcessTuple = checkLockFromAnotherProcess(table_name+"_"+id,transaction_id,"WL",isResourceFree,resource_lock);


        //First check table level lock followed by tuple level lock

        boolean isEdgeAdded = false;
        if(isResourceFree.containsKey(table_name) && writeFromAnotherProcessTable)
        {
            //The only add the edge
            updateLockTable(transaction_id,table_name,lock_table,resource_lock,"RL");
            add_edgeP_util(Integer.valueOf(transaction_id),table_name,isResourceFree);
            isEdgeAdded = true;
        }
        else if(isResourceFree.containsKey(table_name+"_"+id) && writeFromAnotherProcessTuple)
        {
            updateLockTable(transaction_id,table_name+"_"+id,lock_table,resource_lock,"RL");
            add_edgeP_util(Integer.valueOf(transaction_id),table_name+"_"+id,isResourceFree);
            isEdgeAdded = true;
        }
        else
        {
            boolean lockPresent = checkLockFromSameProcess(table_name+"_"+id,transaction_id,"RL",isResourceFree,resource_lock);
            updateAllMaps(lockPresent,table_name+"_"+id,transaction_id,isResourceFree,resource_lock,txn_resources,"RL");
        }
        return isEdgeAdded;
    }

    public static void updateAllMapsAfterVertexRemoval(Integer remove_id,
                                                        HashMap<String, ArrayList<String>>txn_resources,
                                                            HashMap<String,ArrayList<String>>lock_table,
                                                                HashMap<String,ArrayList<Integer>>isResourceFree,
                                                                    HashMap<String,ArrayList<String>>resource_lock)
    {
            for(String resource: txn_resources.get(String.valueOf(remove_id)))
            {
                Integer next_transaction = null;
                boolean isRead = true;

                if(lock_table.containsKey(resource) && lock_table.get(resource).size() != 0)
                {
                    next_transaction = Integer.valueOf(lock_table.get(resource).get(0).split("_")[0]);

                    if(lock_table.get(resource).get(0).split("_")[1].equals("WL"))
                    {
                        isRead = false;
                    }
                }

                //Remove the transaction ID from the resource
                int remove_index = isResourceFree.get(resource).indexOf(remove_id);
                //System.out.println("REMOVE ID IS " + remove_id);
                ArrayList<Integer>temp_isResource = new ArrayList<Integer>();
                ArrayList<String>temp_lock = new ArrayList<String>();

                int i = 0;
                for(Integer txns: isResourceFree.get(resource))
                {
                    if(!txns.equals(remove_id))
                    {
                        temp_isResource.add(txns);
                        temp_lock.add(resource_lock.get(resource).get(i));
                    }
                    i++;
                }

                //Update new isResourceFree and resource_lock
                isResourceFree.put(resource,temp_isResource);
                resource_lock.put(resource,temp_lock);

                if(isResourceFree.get(resource).size() == 0)
                {
                    isResourceFree.remove(resource);
                    resource_lock.remove(resource);
                }

                if(next_transaction == null) continue;

                System.out.println("Next transaction" + next_transaction + " txn resource " + txn_resources.get(String.valueOf(next_transaction)));

                updateAllMaps(false,resource,String.valueOf(next_transaction),isResourceFree,resource_lock,txn_resources,isRead?"RL":"WL");

                System.out.println("Next transaction" + next_transaction + " txn resource " + txn_resources.get(String.valueOf(next_transaction)));

                lock_table.get(resource).remove(0);

                //Add new edges to the graph
                if(lock_table.get(resource).size() == 0)
                    lock_table.remove(resource);
                else
                {
                    for(String txns: lock_table.get(resource))
                    {
                        if(!Integer.valueOf(txns.split("_")[0]).equals(next_transaction))
                            add_edgeP(Integer.valueOf(txns.split("_")[0]),next_transaction);
                    }
                }

            }

            //Remove Everything related to that transaction
            System.out.println("Going to remove " + remove_id);
            txn_resources.remove(String.valueOf(remove_id));
    }

    public static void removeFromLockTable(String transaction_id,
                                            HashMap<String,ArrayList<String>>lock_table)
    {
        ArrayList<String>remove_keys = new ArrayList<String>();
        for(String key: lock_table.keySet())
        {
                ArrayList<String>temp = new ArrayList<String>();
                for(String pendingTxn: lock_table.get(key))
                {
                    if(!pendingTxn.split("_")[0].equals(transaction_id))
                        temp.add(pendingTxn);
                }

                if(temp.size() > 0)
                {
                    System.out.println("Put");
                    lock_table.put(key,temp);
                }
                else
                {
                    remove_keys.add(key);
                    //System.out.println("Remove");
                    //lock_table.remove(key);
                }
        }
        for(String key:remove_keys)
        {
            lock_table.remove(key);
        }
    }

    public static void ReleaseLock(String transaction_id,String table_name, String id,
                                    HashMap<String, ArrayList<String>>txn_resources,
                                        HashMap<String,ArrayList<String>>lock_table,
                                            HashMap<String,ArrayList<Integer>>isResourceFree,
                                                HashMap<String,ArrayList<String>>resource_lock)
    {
        //Release the lock
        //Remove it from the txn_resource
        //Remove a value from isResourceFree
        //Remove corresponding entry from resource_lock
        //Go to the lock table find the next transaction in line

        //I need to iterate through the lock table and remove the edge from curent txn id to all the entries
        //Assign txn_resource, isResourceFree, resource_lock

        //Add the remaining entries point to next transaction in the graph

        String key = table_name+"_"+id;

        System.out.println("The key is " + key);
        System.out.println("Transaction id " + transaction_id);

        int index = -1, i =0;
        for(String resource: txn_resources.get(transaction_id))
        {
            if(resource.equals(key))
            {
                index = i;
                break;
            }
            i++;
        }
        if(index == -1)
        {
            System.out.println("Txn: Sorry no match found");
        }
        else
        {
            txn_resources.get(transaction_id).remove(index);
        }

        index = -1;
        i = 0;
        System.out.println(isResourceFree.get(key));
        System.out.println(resource_lock.get(key));
        for(Integer txn: isResourceFree.get(key))
        {
            if(txn.equals(Integer.valueOf(transaction_id))
                    && resource_lock.get(key).get(i).equals("RL"))
            {
                index = i;
                break;
            }
            i++;
        }

        if(index == -1)
        {
            System.out.println("isResourceFree: Sorry no match found");
        }
        else
        {
            isResourceFree.get(key).remove(index);
            resource_lock.get(key).remove(index);
        }
        /*
        if(txn_resources.get(String.valueOf(transaction_id)).remove(key))
            System.out.println("Successfully removed txN");

        int index = isResourceFree.get(key).indexOf(transaction_id);
        System.out.println("Index is " + index);

        System.out.println("isResourceFree table " + isResourceFree.get(key));

        if(isResourceFree.get(key).remove(transaction_id))
            System.out.println("Successfully removed isResourceFree");

        System.out.println("isResourceFree table " + isResourceFree.get(key));

        resource_lock.get(key).remove(index);

         */


        Integer next_transaction = null;
        boolean isRead = true;

        if(lock_table.containsKey(key) && lock_table.get(key).size() > 0)
        {
            next_transaction = Integer.valueOf(lock_table.get(key).get(0).split("_")[0]);
            if(lock_table.get(key).get(0).split("_")[1].equals("WL"))
                isRead = false;
        }

        System.out.println("The next transaction is " + next_transaction);

        for(String locks: resource_lock.get(key))
        {
            if(locks.equals("WL"))
            {
                next_transaction = null;
                break;
            }
        }

        if(next_transaction == null) return;

        next_transaction = next_transaction;

        for(String waiting_txn: lock_table.get(key))
        {
            //Remov the edge between waiting_txn -> transaction_id

            remove_edgeP_util(Integer.valueOf(waiting_txn.split("_")[0]),Integer.valueOf(transaction_id));
        }

        //Set all the next transaction
        updateAllMaps(false,key,String.valueOf(next_transaction),isResourceFree,resource_lock,txn_resources,isRead?"RL":"WL");

        //Reassign the edges(remove the first entry from the lock table to remove self loops)
        lock_table.get(key).remove(0);

        /*
        //No one is waiting for this resource
        if(lock_table.get(key).size() == 0)
            lock_table.remove(key);

         */

        //Reassign the edges
        for(String waiting_txn: lock_table.get(key))
        {
            String source = waiting_txn.split("_")[0];
            wait_for_graphP.get(Integer.valueOf(source)).add(next_transaction);
        }

    }

    public static void updateCommitOperations(String transaction_id, String operation , String operation_type,
                                            HashMap<Integer,ArrayList<String>>commit_operations,String table_name)
    {
        Integer id = Integer.valueOf(transaction_id);
        if(commit_operations.containsKey(Integer.valueOf(id)))
        {
            commit_operations.get(id).add(operation_type+"_"+table_name+"_"+operation);
        }
        else
        {
            ArrayList<String>temp = new ArrayList<String>();
            temp.add(operation_type+"_"+table_name+"_"+operation);

            commit_operations.put(id,temp);
        }
    }

    public static void scheduleProcess(List processes) {

        //Read Committed: Here read locks are released as soon as it acquired.
        //No atomicity We will commit operations as we go, keep performing operations
        //on the fly for reads but delay writes till commit time. In this way it reads
        //only from committed data but suffers from non-repeatable reads

        init_graphP();
        boolean isFinalCycle = false;

        //Another hashmap where key is the transaction ID and value is the list of resources it is holding
        HashMap<String, ArrayList<String>>txn_resources = new HashMap<String,ArrayList<String>>();

        //Lock table is hashmap of resources waiting for access when a transaction holding that resource commits
        HashMap<String,ArrayList<String>>lock_table = new HashMap<String,ArrayList<String>>();


        //Hashmap key as resource and the value can {RL,WL}
        HashMap<String,ArrayList<Integer>>isResourceFree = new HashMap<String,ArrayList<Integer>>();
        HashMap<String,ArrayList<String>>resource_lock = new HashMap<String,ArrayList<String>>();

        //List of active transactions
        ArrayList<Integer>txn_ids = new ArrayList<Integer>();

        //Create a abort list
        ArrayList<String>abort_list = new ArrayList<String>();
        ArrayList<String>commit_list = new ArrayList<String>();

        int no_vertices = 0;

        System.out.println("_________________History______________");
        for(Object t:processes)
        {
            System.out.println(t);
        }
        System.out.println("_______________________________________");



        for(Object t:processes)
        {
            String transaction_id = t.toString().split(",")[0].substring(1);

            if(abort_list.contains(transaction_id)) continue;

            if(!txn_ids.contains(Integer.valueOf(transaction_id)))
            {
                txn_ids.add(Integer.valueOf(transaction_id));
                no_vertices++;
            }

            String operation = t.toString().substring(3).trim();


            operation = operation.substring(0,operation.length()-1);

            System.out.println(operation);

            if(operation.charAt(0) == 'R')
            {
                //System.out.println(operation);

                String table_name = operation.split(" ")[1];
                String id = operation.split(" ")[2];

                //This will try to acquire locks if possible else add edges to the graph
                if(!checkLevelReadLocks(transaction_id,table_name,id,txn_resources,lock_table,isResourceFree,resource_lock))
                {
                    //Perform the operation and release the lock
                    Memory.readID(table_name,id);

                    //Release the read locks
                    ReleaseLock(transaction_id,table_name,id,txn_resources,lock_table,isResourceFree,resource_lock);
                }
                else
                {
                   updateCommitOperations(transaction_id,id,"R",commit_operations,table_name);
                }

            }
            else if(operation.charAt(0) == 'M')
            {
                String table_name = operation.split(" ")[1];
                String area = operation.split(" ")[2];

                ArrayList<Integer>id_list = new ArrayList<Integer>();
                id_list = Memory.returnAreaCodes(table_name,area);

                for(int i=0;i<id_list.size();i++)
                {
                    if(!checkLevelReadLocks(transaction_id,table_name,String.valueOf(id_list.get(i)),txn_resources,lock_table,isResourceFree,resource_lock))
                    {
                            Memory.readID(table_name,String.valueOf(id_list.get(i)));
                            ReleaseLock(transaction_id,table_name,String.valueOf(id_list.get(i)),txn_resources,lock_table,isResourceFree,resource_lock);
                    }
                    else
                    {
                        updateCommitOperations(transaction_id,String.valueOf(id_list.get(i)),"R",commit_operations,table_name);
                    }
                }

                //Perform the operation




            }
            else if(operation.charAt(0) == 'W')
            {
                System.out.println("OPERATION WRITE " + operation);
                String table_name = operation.split(" ")[1];
                String id = operation.substring(5,operation.length()-1).split(",")[0];

                if(!checkLevelWriteLocks(transaction_id,table_name,id,txn_resources,lock_table,isResourceFree,resource_lock))
                    Memory.write(table_name,operation.substring(3,operation.length()));
                else
                {
                    //Wait till commit time
                    updateCommitOperations(transaction_id,operation.substring(3,operation.length()).trim(),"W",commit_operations,table_name);
                }

                //Peform the operation

            }
            else if(operation.charAt(0) == 'E')
            {

                String table_name = operation.split(" ")[1];
                String id = operation.split(" ")[2];

                if(!checkLevelWriteLocks(transaction_id,table_name,id,txn_resources,lock_table,isResourceFree,resource_lock))
                    Memory.erase(table_name,operation.trim());
                else
                {
                    //Delay to commit time
                    updateCommitOperations(transaction_id,operation.trim(),"E",commit_operations,table_name);
                }
                //System.out.println("operation "+operation);
                //Perform the operation but do not release the lock

            }
            else if(operation.charAt(0) == 'D')
            {
                //Delete the table itself

                //Check if there are any tuple level or table level locks on the table
                //If there is a locks then add edge

                String table_name  = operation.split(" ")[1];
                boolean isConflict = false;
                for(String key: isResourceFree.keySet())
                {
                    if(key.equals(table_name) || key.split("_")[0].equals(table_name))
                    {
                        updateLockTable(transaction_id,table_name,lock_table,resource_lock,"WL");
                        add_edgeP_util(Integer.valueOf(transaction_id),key,isResourceFree);
                        isConflict = true;
                    }
                }

                //If isConflict is true then(Even if one edge is added we cannot access the lock)
                if(!isConflict)
                {
                    boolean lockPresent  = checkLockFromSameProcess(table_name,transaction_id,"WL",isResourceFree,resource_lock);
                    updateAllMaps(lockPresent,table_name,transaction_id,isResourceFree,resource_lock,txn_resources,"WL");
                    Memory.delete(table_name);
                }
                else
                {
                    updateCommitOperations(transaction_id,table_name,"D",commit_operations,table_name);
                }

            }
            else if(operation.charAt(0) == 'C')
            {
                //Upon commit and abort we still need to reassign the edges

                //Check if the transaction has outgoing edges.
                //If it has outgoing edges then delay the commit(meaning put it in the list)
                if(wait_for_graphP.get(Integer.valueOf(transaction_id)).size() > 0)
                {
                    commit_list.add(transaction_id);
                }
                else if(!abort_list.contains(transaction_id)
                        && commit_operations.containsKey(Integer.valueOf(transaction_id)))
                {
                    System.out.println("Commiting all the delayed operations");
                    for(String op: commit_operations.get(Integer.valueOf(transaction_id)))
                    {
                        System.out.println(op);
                        String op_type = op.split("_")[0];
                        String table_name = op.split("_")[1];
                        String data = op.split("_")[2];

                        if(op_type.equals("R"))
                            Memory.readID(table_name,data);
                        else if(op_type.equals("W"))
                            Memory.write(table_name,data);
                        else if(op_type.equals("E"))
                            Memory.erase(table_name,data);
                        else if(op_type.equals("D"))
                            Memory.delete(table_name);
                    }

                    removeVertex(Integer.valueOf(transaction_id),no_vertices);
                    updateAllMapsAfterVertexRemoval(Integer.valueOf(transaction_id),txn_resources,lock_table,isResourceFree,resource_lock);
                }


                //System.out.println(transaction_id+" is about to commit");
                //removeVertex(Integer.valueOf(transaction_id),no_vertices);
                //updateAllMapsAfterVertexRemoval(Integer.valueOf(transaction_id),txn_resources,lock_table,isResourceFree,resource_lock);

            }
            else if(operation.charAt(0) == 'A')
            {
                //Upon commit and abort we still need to reassign the edges
                removeVertex(Integer.valueOf(transaction_id),no_vertices);
                updateAllMapsAfterVertexRemoval(Integer.valueOf(transaction_id),txn_resources,lock_table,isResourceFree,resource_lock);
                removeFromLockTable(transaction_id,lock_table);
                abort_list.add(transaction_id);
            }
            else
            {
                System.out.println("Not a valid operation(R/M/W/E/D)");
            }

            System.out.println("______________TXN resources ___________");
            for(String keys: txn_resources.keySet())
            {
                System.out.println("_"+keys + "_ " + txn_resources.get(keys));
            }
            System.out.println("______________________________________");

            System.out.println("______________Lock Table ___________");
            for(String keys: lock_table.keySet())
            {
                System.out.println("_"+keys + "_ " + lock_table.get(keys));
            }
            System.out.println("______________________________________");

            System.out.println("______________isResource Table ___________");
            for(String keys: isResourceFree.keySet())
            {
                System.out.println("_"+keys + "_ " + isResourceFree.get(keys));
            }
            System.out.println("______________________________________");

            System.out.println("______________Resource lock Table ___________");
            for(String keys: resource_lock.keySet())
            {
                System.out.println("_"+keys + "_ " +resource_lock.get(keys));
            }
            System.out.println("______________________________________");

            System.out.println("_________The adjacency list for this graph_____________ "+wait_for_graphT.size());
            for(int i=0;i<=no_vertices+1;i++)
            {
                System.out.print(i+" ");
                for(Integer neighbour: wait_for_graphP.get(i))
                {
                    System.out.print(","+neighbour);
                }
                System.out.println();
            }

            System.out.println("_____________Cycle Detection_________________");
            boolean[] visited = new boolean[TOT_VERTICES];
            boolean[] inCurStack = new boolean[TOT_VERTICES];
            boolean is_cycle = false;
            for(int i=1;i<=no_vertices+1;i++)
            {
                //Check if the transaction is alive
                //if(txn_resources.containsKey(i))
                //System.out.println("Transaction " + i + " is present");

                if(txn_ids.contains(i) && isCycleP(i,visited,inCurStack))
                {
                    //System.out.println("Deadlock Detected");
                    is_cycle = true;

                    //Remove the transaction that had caused this deadlock(last transaction) choose as victim
                    System.out.println("Deadlock has been detected");
                    System.out.println("Choosing " + transaction_id + " as the victim");
                    removeVertex(Integer.valueOf(transaction_id),no_vertices);
                    updateAllMapsAfterVertexRemoval(Integer.valueOf(transaction_id),txn_resources,lock_table,isResourceFree,resource_lock);
                    removeFromLockTable(transaction_id,lock_table);
                    abort_list.add(transaction_id);
                }
            }

            if(is_cycle)
            {
                isFinalCycle = true;
                System.out.println("Deadlock has been detected in this history");

                //break;
            }
            else
            {
                //System.out.println("There is no deadlock in this history");
            }
            System.out.println("____________________________________________");


        }

        System.out.println("Commiting all process that was delayed due to dependency");
        System.out.println(commit_list.size());
        for(String txn: commit_list)
        {
            for(String op:commit_operations.get(Integer.valueOf(txn)))
            {
                String op_type = op.split("_")[0];
                String table_name = op.split("_")[1];
                String data = op.split("_")[2];

                if(op_type.equals("R"))
                    Memory.readID(table_name,data);
                else if(op_type.equals("W"))
                    Memory.write(table_name,data);
                else if(op_type.equals("E"))
                    Memory.erase(table_name,data);
                else if(op_type.equals("D"))
                    Memory.delete(table_name);

                removeVertex(Integer.valueOf(txn),no_vertices);
                updateAllMapsAfterVertexRemoval(Integer.valueOf(txn),txn_resources,lock_table,isResourceFree,resource_lock);
            }
        }




    }

}

