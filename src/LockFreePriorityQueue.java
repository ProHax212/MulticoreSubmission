import javafx.util.Pair;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;

/**
 * A lock free priority queue
 * The algorithm that was used was: "Fast and lock-free concurrent priority queues for multi-thread systems"
 * Authors: "Hakan Sundell, Philippas Tsigas"
 * Link - http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.67.1310&rep=rep1&type=pdf
 */
public class LockFreePriorityQueue {

    static final int MAX_LEVEL = 25;    // Maximum height of the skiplist

    // Sentinel Head and tail nodes (-Infinity : +Infinity)
    final Node head = new Node(MAX_LEVEL+1, Integer.MIN_VALUE, Integer.MIN_VALUE);
    final Node tail = new Node(MAX_LEVEL+1, Integer.MAX_VALUE, Integer.MAX_VALUE);

    Random r = new Random();    // Used for randomly assigning a level to a node

    public LockFreePriorityQueue(){
        for(int i = 0; i < head.next.length; i++){
            head.next[i] = new AtomicMarkableReference<Node>(tail, false);
        }
    }

    // Get a random level with a geometric distribution
    private int randomLevel(){
        int startingLevel = 0;
        while (startingLevel < MAX_LEVEL){
            if(r.nextBoolean()) startingLevel += 1;
            else break;
        }

        return startingLevel;
    }

    // Return false if the node is marked for deletion, otherwise return the reference
    private Node readNode(AtomicMarkableReference<Node> node){
        if(node.isMarked()) return null;
        else return node.getReference();
    }

    // Physically remove a node from the skiplist at the given level
    //
    private void removeNode(Node node, Node prev, int level){
        Node last;

        while (true){
            // Already removed
            if(node.next[level].getReference() == null && node.next[level].isMarked()) break;

            // Find right position of previous node
            Wrapper w = scanKey(prev, level, node.key);
            last = w.node;
            prev = w.prev;

            // Verify node is still part of linked list
            if((last != node) || (node.next[level].getReference() == null && node.next[level].isMarked())) break;
            // My previous node points to my next node (i'm removed)
            if(prev.next[level].compareAndSet(node, node.next[level].getReference(), false, false)){
                node.next[level] = new AtomicMarkableReference<>(null, true);
                break;
            }

            if(node.next[level].getReference() == null && node.next[level].isMarked()) break;

            Thread.yield(); // Back off
        }
    }

    // Delete at the current level - return reference to previous node
    private Node helpDelete(Node node, int level){
        Node last, node2, prev;
        boolean node2Marked;

        // Set deletion on all next pointers at higher levels
        node2 = new Node(0,0,0);
        for(int i = level; i <= node.level - 1; i++){
            // Repeat until successfully sets next ptr to marked
            do{
                node2 = node.next[i].getReference();
                node2Marked = node.next[i].isMarked();
            }while(!node2Marked && (!node.next[i].compareAndSet(node2, node2, false, true)));
        }

        prev = node.prev;

        // Make sure prev is valid for deletion
        // If not, search for correct previous node
        if(prev == null || level >= prev.validLevel){
            prev = head;
            // Search for correct previous node
            for(int i = MAX_LEVEL-1; i >= level; i--) {
                Wrapper w = scanKey(prev, i, node.key);
                node2 = w.node;
                prev = w.prev;
            }
        }

        // Delete the node at the current level
        removeNode(node, prev, level);  // Remove the node at the level
        return prev;
    }

    // Return the next node helping nodes that need to be deleted
    private Wrapper readNext(Node node1, int level){
        Node node2;

        // Marked - help delete the node
        if(node1.marked.get()) node1 = helpDelete(node1, level);
        node2 = readNode(node1.next[level]);

        // Keep reading nodes until they aren't Null
        // Helpdelete them if they're null
        while(node2 == null){
            node1 = helpDelete(node1, level);
            node2 = readNode(node1.next[level]);
        }

        return new Wrapper(node2, node1);
    }

    // Find a node on the current level that has the same(or higher) key
    private Wrapper scanKey(Node node1, int level, int key){
        Node node2;
        Wrapper w = readNext(node1, level);
        node1 = w.prev;
        node2 = w.node;

        // Loop while key is less
        while(node2.key < key){
            node1 = node2;
            w = readNext(node1, level);
            node1 = w.prev;
            node2 = w.node;
        }

        return w;
    }

    // Enqueue a value/priority pair into the queue
    public boolean insert(Integer value, int key){
        Node node1, node2, newNode;
        Node savedNodes[] = new Node[MAX_LEVEL];
        int level = randomLevel();
        if(level == 0) level = 1;
        if(level >= MAX_LEVEL) level = MAX_LEVEL - 1;
        newNode = new Node(level, key, value);
        node1 = head;

        // Loop through the levels
        for(int i = MAX_LEVEL - 1; i >= 1; i--){
            // Find where to put node at this level
            Wrapper w = scanKey(node1, i, key);
            node2 = w.node;
            node1 = w.prev;
            // Remember last node at the level for later use
            if(i < level) savedNodes[i] = node1;
        }

        while(true){
            // Find where to insert at lowest level
            Wrapper w = scanKey(node1, 0, key);
            node1 = w.prev;
            node2 = w.node;
            Integer value2 = node2.value;
            // Found the same key, update the value
            if(!node2.marked.get() && node2.key == key){
                return false;
            }

            // Add at lowest level
            newNode.next[0] = new AtomicMarkableReference<Node>(node2, false);
            if(node1.next[0].compareAndSet(node2, newNode, false, false)) break;

            Thread.yield(); // Back off
        }

        // Insert at higher levels
        for(int i = 1; i <= level-1; i++){
            newNode.validLevel = i;
            node1 = savedNodes[i];
            while(true){
                Wrapper w = scanKey(node1, i, key);
                node1 = w.prev;
                node2 = w.node;
                newNode.next[i] = new AtomicMarkableReference<>(node2, false);
                // New node was deleted at lowest level
                if(newNode.marked.get() || node1.next[i].compareAndSet(node2, newNode, node1.next[i].isMarked(), node1.next[i].isMarked())) break;
                Thread.yield(); // Back off
            }
        }
        newNode.validLevel = level;

        // New node deleted at lowest level
        if(newNode.marked.get()){
            newNode = helpDelete(newNode, 0);
        }

        return true;
    }

    // Pop off the top priority in the queue
    public Integer deleteMin(){
        Node prev = head;
        Node node1 = new Node(0, 0, 0); Node node2;
        Integer valueRef;

        // Loop until you find the first node thats not marked
        boolean retry = false;  // Used to simulate the goto operation in the psuedocode
        while(true){
            if(!retry) {
                Wrapper w = readNext(prev, 0);
                prev = w.prev;
                node1 = w.node;

                // Node is the tail
                if (node1 == tail) return null;
            }

            retry = true;
//            boolean valueMark = node1.value.isMarked();
//            valueRef = node1.value.getReference();

            //Node isn't the next pointer of prev - continue
            if(node1 != prev.next[0].getReference()){
                retry = false;
                continue;
            }

            // Node wasn't marked for deletion
            if(!node1.marked.get()){
                // Mark for deletion
                if(node1.marked.compareAndSet(false, true)){
                    node1.prev = prev;   // Set previous for better time
                    break;
                }else continue;
            }
            // Node was marked, help delete
            else{
                node1 = helpDelete(node1, 0);
            }

            // Didn't find an unmarked node - continue
            prev = node1;
            retry = false;
        }

        // Mark all of the next pointers
        boolean node2Marked;
        for(int i = 0; i <= node1.level-1; i++){
            // Keep trying until you mark next ptr
            do{
                node2 = node1.next[i].getReference();
                node2Marked = node1.next[i].isMarked();
            }while(!node2Marked && !node1.next[i].compareAndSet(node2, node2, false, true));
        }

        prev = head;

        // Remove the nodes starting from the top
        for(int i = node1.level-1; i >= 0; i--){
            removeNode(node1, prev, i);
        }

        return node1.value;
    }

    // Node class for nodes in the skiplist
    private static class Node{
        int key, level, validLevel;
        Integer value; // Mark for the current node
        Node prev;
        AtomicMarkableReference<Node> next[];
        AtomicBoolean marked;

        // Constructor for normal Nodes
        public Node(int level, int key, int value){
            prev = null;
            marked = new AtomicBoolean(false);
            validLevel = 0;
            this.level = level;
            this.key = key;
            this.value = value;
            this.next = new AtomicMarkableReference[level + 1];
            for(int i = 0; i < this.next.length; i++){
                next[i] = new AtomicMarkableReference<>(null, false);
            }
        }

        // Constructor for sentinel nodes
        public Node(int key){
            validLevel = 0;
            prev = null;
            value = null;
            this.key = key;
            this.next = new AtomicMarkableReference[MAX_LEVEL+1];
            for(int i = 0; i < next.length; i++){
                next[i] = new AtomicMarkableReference<Node>(null, false);
            }
            level = MAX_LEVEL;
        }
    }

    // Wrapper class used so ReadNext and ScanKey can return the node as well as previous
    private static class Wrapper{
        Node node, prev;

        public Wrapper(Node node, Node prev){
            this.node = node;
            this.prev = prev;
        }
    }

    // To string method for debugging
    public String toString(){
        String returnString = "\n";
        int level = MAX_LEVEL-1;
        Node node1 = head;

        while(level >= 0){
            Node node2 = node1.next[level].getReference();
            returnString += head.key + ", ";
            while(node2 != tail){
                returnString += node2.key + ", ";
                node2 = node2.next[level].getReference();
            }
            returnString += tail.key + ", ";
            level -= 1;
            returnString += "\n";
        }

        returnString += "\n";
        return returnString;
    }

    // Maker sure the ordering at each level is decreasing with with priority (higher keys)
    public boolean verify(){
        int level = MAX_LEVEL-1;

        while(level >= 0){
            Node node1 = head;
            Node node2 = node1.next[level].getReference();
            while(node2 != tail){
                if (node2.key < node1.key) return false;
                node2 = node2.next[level].getReference();
            }
            level -= 1;
        }

        return true;
    }

}
