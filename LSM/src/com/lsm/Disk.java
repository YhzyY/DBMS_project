package com.lsm;

import java.io.*;
import java.util.*;

public class Disk {

    static int SSTableCapacity;
    static int levels;

    public Disk(int ssTableCapacity) {
        this.SSTableCapacity = ssTableCapacity;
        levels = 0;
    }

    public static int getLevels() {
        return levels;
    }

    public static int getCapacity() {
        return SSTableCapacity;
    }

    public static boolean isFull(int level) {
        try {
            File file = new File("/Users/ziyi/document/2020spring/CS2550/hw/DBMS_project/LSM");
            File[] f = file.listFiles();
            int count = 0;
            for (File file2 : f) {
                String path = file2.getAbsolutePath();
                String name = path.substring(path.lastIndexOf("/") + 1);
                String s = level + "";
                if (name.substring(0, 1).equals(s)) {
                    count++;
                }
            }
            if (level == 0) {
                if (count > 4) return true;
                else return false;
            } else if (level == 1) {
                if (count > 10) return true;
                else return false;
            } else {
                if (count > Math.pow(10, level)) return true;
                else return false;
            }
        } catch (Exception e) {
        }
        return false;
    }

    public static boolean isEmpty(int level) {
        try {
            File file = new File("/Users/ziyi/document/2020spring/CS2550/hw/DBMS_project/LSM");
            File[] f = file.listFiles();
            int count = 0;
            for (File file2 : f) {
                String path = file2.getAbsolutePath();
                String name = path.substring(path.lastIndexOf("/") + 1);
                String s = level + "";
                if (name.substring(0, 1).equals(s)) {
                    count++;
                }
            }
            if (count == 0) return true;
            else return false;
        } catch (Exception e) {
        }
        return false;
    }

    public static int numTable(int level) {

        try {
            File file = new File("/Users/ziyi/document/2020spring/CS2550/hw/DBMS_project/LSM");
            File[] f = file.listFiles();
            int count = 0;
            for (File file2 : f) {
                String path = file2.getAbsolutePath();
                String name = path.substring(path.lastIndexOf("/") + 1);
//                System.out.println(name + " ===========");
                String s = level + "";
                if (name.substring(0, 1).equals(s)) {
                    count++;
                }
            }
            return count;
        } catch (Exception e) {
        }
        return 0;
    }

    public static int numTableforTable(int level, String tableName) {

        try {
            File file = new File("/Users/ziyi/document/2020spring/CS2550/hw/DBMS_project/LSM");
            File[] f = file.listFiles();
            int count = 0;
            for (File file2 : f) {
                String path = file2.getAbsolutePath();
                String name = path.substring(path.lastIndexOf("/") + 1);
//                System.out.println(name + " ===========name");
                String s = level + "" + tableName;
                if (name.substring(0, s.length()).equals(s)) {
                    count++;
                }
            }
            return count;
        } catch (Exception e) {
        }
        return 0;
    }

    public static void Compact(int level) {
        File file = new File("/Users/ziyi/document/2020spring/CS2550/hw/DBMS_project/LSM");
        File[] f = file.listFiles();
        List<String> table = new ArrayList<>();
        for (File file2 : f) {
            String path = file2.getAbsolutePath();
            String name = path.substring(path.lastIndexOf("/") + 1);
            String s = level + "";
            if (name.substring(0, 1).equals(s)) {
                if (!table.contains(name.substring(1, 2))) {
                    table.add(name.substring(1, 2));
                }
            }
        }
        for (int i = 0; i < table.size(); i++) {
            TreeMap merge = new TreeMap<Integer, List>();
            TreeMap treeMap = new TreeMap();
            if (!isEmpty(level + 1)) {
                for (File file2 : f) {
                    String path = file2.getAbsolutePath();
                    String name = path.substring(path.lastIndexOf("/") + 1);
                    String s1 = (level + 1) + "";
                    try {
                        if (name.substring(0, 1).equals(s1) && name.substring(1, 2).equals(table.get(i))) {
                            FileInputStream fis = new FileInputStream(path);
                            ObjectInputStream ois = new ObjectInputStream(fis);
                            treeMap = (TreeMap) ois.readObject();
                            for (Object key : treeMap.keySet()) {
                                merge.put(key, treeMap.get(key));
                            }
                            ois.close();
                            fis.close();
                            file2.delete();
                        }
                    } catch (Exception e) {
                    }
                }
                for (Object key : merge.keySet()) {
                    System.out.println("table: " + key);
                    System.out.println("memtable: " + merge.get(key));
                }
            } else {
                levels++;
            }
            for (File file2 : f) {
                String path = file2.getAbsolutePath();
                String name = path.substring(path.lastIndexOf("/") + 1);
                String s = level + "";
                try {
                    if (name.substring(0, 1).equals(s) && name.substring(1, 2).equals(table.get(i))) {
                        FileInputStream fis = new FileInputStream(path);
                        ObjectInputStream ois = new ObjectInputStream(fis);
                        treeMap = (TreeMap) ois.readObject();
                        for (Object key : treeMap.keySet()) {
                            merge.put(key, treeMap.get(key));
                        }
                        ois.close();
                        fis.close();
                        file2.delete();
                    }
                } catch (Exception e) {
                }
            }
            int iter = 0;
            int count = 0;
            TreeMap split = new TreeMap<Integer, List>();
            for (Object key : merge.keySet()) {
                split.put(key, merge.get(key));
                iter++;
                if (iter == SSTableCapacity) {
                    iter = 0;
                    count++;
                    try {
                        FileOutputStream fos = new FileOutputStream("" + (level + 1) + table.get(i) + count + ".txt");
                        ObjectOutputStream oos = new ObjectOutputStream(fos);
                        oos.writeObject(split);
                        oos.close();
                        fos.close();
                    } catch (Exception e) {
                    }
                    split.clear();
                }
            }
        }
        if (isFull(level + 1)) {
            Compact(level + 1);
        }
    }
}
