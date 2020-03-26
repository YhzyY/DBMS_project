package com.lsm;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class ListNode {

    String key;
    List val;
    String tableName;
//    TreeMap val;
    ListNode next;
    public ListNode() {
        this.key = "";
        this.val = new ArrayList<>();
//        this.val = new TreeMap();
        this.tableName = "";
        this.next = null;
    }
//    public ListNode(String tableName, TreeMap val) {
    public ListNode(String tableName, String key, List val) {
        this.key = key;
        this.val = val;
        this.next = null;
        this.tableName = tableName;
    }
}
