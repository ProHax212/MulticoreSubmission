import java.util.Random;

/**
 * Created by Ryan_Comer on 11/3/2016.
 */
public class SequentialSkipList {

    // Both are sentinel nodes
    public Node head;   // Head of the list
    public Node tail;   // Last element in the list

    public int numEntries;  // Number of entries in the list
    public int height;  // Number of levels in the list
    public Random r;    // Use randomization to determine the height of a newly added node

    // Constructor - create an empty list
    public SequentialSkipList(){
        Node n1 = new Node(Integer.MIN_VALUE, null);
        Node n2 = new Node(Integer.MAX_VALUE, null);

        // Link the nodes
        n1.right = n2;
        n2.left = n1;

        // Initialize head/tail
        head = n1;
        tail = n2;

        // Other init
        numEntries = 0;
        height = 0;
        r = new Random();
    }

    // Find an entry in the list for the given key
    public Node find(int key){
        Node n;

        // Start at the head
        n = head;

        // Keep going until you either find the node or its not there
        while(true){
            // Move right while the right key is smaller
            while(n.right.key <= key) n = n.right;

            // Go down if you can
            if(n.down != null) n = n.down;
            else break; // Done
        }

        return n;   // Note that n might not be the desired node, but the key is guaranteed to be <= what you were looking for
    }

    // Returns a value associated with a key
    public Integer get(int key){
        Node n = find(key);

        // Returned node might not be the right key (see find method)
        if(n.key == key) return n.value;
        else return null;
    }

    // Insert a new value into the list
    public boolean insert(int key, int value){
        Node p, q;
        int i;  // Height of the new entry - need to add a new empty layer if this is equal to the current height of the list

        p = find(key);  // Find the spot for the new node

        // Can't add same key twice
        if(p.key == key) return false;

        // Create the new entry
        q = new Node(key, value);

        // Insert q between p and p's old successor
        q.left = p;
        q.right = p.right;
        p.right.left = q;
        p.right = q;

        // Start making a tower of random height for the new node
        i = 0;  // Initial height of the new tower

        // Randomly increase the height
        while(r.nextBoolean()){
            // Reached to top level
            if(i >= this.height){
                // Create a new empty top layer
                Node n1 = new Node(Integer.MIN_VALUE, null);
                Node n2 = new Node(Integer.MAX_VALUE, null);

                // Link the nodes
                n1.right = n2;
                n2.left = n1;
                n1.down = head;
                n2.down = tail;

                // Update head and tail
                head.up = n1;
                tail.up = n2;
                head = n1;
                tail = n2;

                this.height += 1;   // One more level
            }

            // Find first element with an UP-link
            while(p.up == null) p = p.left;
            p = p.up;   // Swing p over to point to this UP-link

            // Add the next level to the new node's tower at this level
            Node e = new Node(key, value);
            e.left = p;
            e.right = p.right;
            e.down = q;
            // Change the neighboring links
            p.right.left = e;
            p.right = e;
            q.up = e;

            q = e;  // Set q equal to e for the next iteration
            i += 1; // Increase current level by 1
        }

        this.numEntries += 1; // One more entry
        return true;
    }

    // Remove a node
    public Integer remove(int key){
        Node p = find(key);
        if(p.key != key) return null;   // Not found, return null

        // Remove the entire collumn
        Integer returnValue = p.value;
        while(p != null){
            p.left.right = p.right;
            p.right.left = p.left;

            p = p.up;
        }

        return returnValue;
    }

    // Entry for the Skip List
    private static class Node{
        public Integer value;
        public int key; // The key is what the skip list is sorted on

        // 4 Pointers for pointint to adjacent nodes
        public Node up, down, left, right;

        // Constructor - create a new Node with the key/value pair
        public Node(int key, Integer value){
            this.key = key;
            this.value = value;
        }
    }

    // Get the minimum key of the list
    public int getMinKey(){
        Node p = head;
        while(p.down != null) p = p.down;
        return p.right.key;
    }

    // Delete the node with the minimum key
    public Integer deleteMin(){
        return remove(getMinKey());
    }

    // Print method for the Skip list
    public void print(){
        int level = height;
        Node p = head, q = head.right;
        while(level >= 0){
            System.out.print(level + ":\t" + p.key + "\t");
            while(q != null){
                System.out.print(q.key + "\t");
                q = q.right;
            }
            System.out.println();
            if(p.down == null) break;
            p = p.down;
            q = p.right;
            level -= 1;
        }
    }

}
