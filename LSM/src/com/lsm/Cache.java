package com.lsm;
import java.util.*;

public class Cache {

    public int capacity, size;
    public ListNode dummy, tail;
    public Map<ListNode, ListNode> mapToPrev;

    //Cache是由ListNode结构组成
    //每一个node具有多个属性，其中value值为一个treemap（即sstable）
    //每个node用以相互区分的键值由tableName和key共同组成
    public Cache(int capacity) {
        this.size = 0;
        this.capacity = capacity;
        this.dummy = new ListNode();
        this.tail = this.dummy;
        this.mapToPrev = new HashMap<>();
    }

    //遍历整个cache内的所有node，找到tableName和key同时符合的treemap返回
    //找到的话，同时还要将该sstable移至队尾
    public List get(String tableName, String key) {
        ListNode node = dummy.next;
        while(node != null) {
            if(!((node.tableName).equals(tableName))) {
                node = node.next;
                continue;
            }
            if(node.key.equals(key)) {
                moveToTail(node);
                return tail.val;
            }
            node = node.next;
        }
        return null;
    }

    //找到想要的sstable，说明list不可能为空，即tail也不可能为空
    public void moveToTail(ListNode node) {
        if(tail == node) {
            return;
        }

        //原序列： dummy->head->...->prev->node->node.next->...->tail->null
        //更改后： dummy->head->...->prev->node.next->...->tail->node->null 然后再将tail指向node
        ListNode prev = mapToPrev.get(node);
        prev.next = node.next;
        mapToPrev.put(node.next, prev);
        mapToPrev.put(node, tail);
        tail.next = node;
        tail = node;
        node.next = null;
    }

    //如果是从disk里找到的sstable，就要存入cache
    //如果当前cache没有满，直接加入
    //如果当前cache已满（size==capacity)，先删除头节点，然后将新的sstable加入列表尾部
    public void add(String tableName, String key, List value) {
//        System.out.println("Adding node (" + tableName + ": " + value.toString());
        ListNode newNode = new ListNode(tableName, key, value);
        if(size == capacity) {
            System.out.println("SWAP K-" + tableName + key);
            mapToPrev.remove(dummy.next);
            dummy.next = dummy.next.next;       //若capacity=1，dummy->node->null. 所以也不会出错
            if(dummy.next != null) {
                mapToPrev.put(dummy.next, dummy);
            } else {
                tail = dummy;
            }
            size--;
        }

        mapToPrev.put(newNode, tail);
        tail.next = newNode;            //若size=0,则dummy和tail均为null，且有tail=dummy，即有dummy.next=newNode
        tail = newNode;
        size++;
    }

    public void set(String tableName, String key, List value) {
        //TODO
        ListNode node = dummy.next;
        while(node != null) {
            if(!((node.tableName).equals(tableName))) {
                node = node.next;
                continue;
            }
            if(node.key.equals(key)) {
                node.val = value;
//                moveToTail(node);
            }
            node = node.next;
        }
    }

}
