import java.util.Random;
import java.util.concurrent.atomic.AtomicMarkableReference;

/**
 * Created by Ryan_Comer on 11/5/2016.
 */
public class LockFreeSkipList {

    static final int MAX_LEVEL = 10;
    final Node head = new Node(Integer.MIN_VALUE);
    final Node tail = new Node(Integer.MAX_VALUE);

    private Random r = new Random();

    public LockFreeSkipList(){
        for(int i = 0; i < head.next.length; i++){
            head.next[i] = new AtomicMarkableReference<Node>(tail, false);
        }
    }

    // Try to find the key, return true if found
    // Populate the preds[] and succs[] arrays for the key
    private boolean find(int x, Node[] preds, Node[] succs){
        int bottomLevel = 0;
        int key = x;
        boolean[] marked = {false};
        boolean snip;
        Node pred=null, curr=null, succ=null;

        // Keep trying
        retry:
        while(true){
            pred = head;
            // Start from the top
            for(int level = MAX_LEVEL; level >= bottomLevel; level--){
                curr = pred.next[level].getReference(); // Current node
                while(true){
                    succ = curr.next[level].get(marked);    // Get the successor - marked array holds the marked variable
                    // Successor was marked - remove the link
                    while(marked[0]){
                        snip = pred.next[level].compareAndSet(curr, succ, false, false);    // Pred now points to succ, thus removing current
                        if(!snip) continue retry;   // CAS failed, start from beginning of find
                        curr = pred.next[level].getReference();
                        succ = curr.next[level].get(marked);
                    }
                    // Keep moving down the shortcut list
                    if(curr.key < key){
                        pred = curr;
                        curr = succ;
                    }else break;    // Parameter key is smaller than current key
                }

                // Update preds and succs at this level for return
                preds[level] = pred;
                succs[level] = curr;
            }
            return (curr.key == key);
        }
    }

    public boolean add(int x){
        int topLevel = r.nextInt(MAX_LEVEL+1);
        int bottomLevel = 0;
        Node[] preds = new Node[MAX_LEVEL+1];
        Node[] succs = new Node[MAX_LEVEL+1];

        // Loop until successful
        while(true){
            boolean found = find(x, preds, succs);  // Updates the preds and succs arrays.  True if the key is already there
            if(found) return false; // Can't add same node twice
            else{
                Node newNode = new Node(x, topLevel);
                // Link all of the successor pointers before CAS
                for(int level=bottomLevel; level <= topLevel; level++){
                    Node succ = succs[level];
                    newNode.next[level].set(succ, false);   // Make link to successor at the level
                }
                Node pred = preds[bottomLevel];
                Node succ = succs[bottomLevel];
                // Try to link to predecessor at bottom level
                if(!pred.next[bottomLevel].compareAndSet(succ, newNode, false, false)) continue;    // Failed, try the whole process again

                // Link successful -> try to link at higher levels
                // Its ok for the higher levels to be unlinked, they're just shortcuts
                for(int level = bottomLevel+1; level < topLevel; level++){
                    // Try until successful
                    while(true){
                        pred = preds[level];
                        succ = succs[level];
                        if(pred.next[level].compareAndSet(succ, newNode, false, false)) break;
                        // Failed, need to do find again to update preds and succs
                        find(x, preds, succs);
                    }
                }

                return true;
            }
        }
    }

    // Remove the node with the given key from the list
    public boolean remove(int x){
        int bottomLevel = 0;
        Node[] preds = new Node[MAX_LEVEL+1];
        Node[] succs = new Node[MAX_LEVEL+1];
        Node succ;

        // Keep trying
        while(true){
            boolean found = find(x, preds, succs);
            if(!found) return false;    // Node isn't there
            else{
                Node nodeToRemove = succs[bottomLevel]; // Mark the successor to say the current node should be removed
                // Mark the succ at every level starting from the top (excluding bottom level
                for(int level=nodeToRemove.topLevel; level >= bottomLevel+1; level--){
                    boolean[] marked = {false}; // Holds the marked variable from .get()
                    succ = nodeToRemove.next[level].get(marked);
                    // Continue while succ isn't marked.  If it is marked then you don't need to re-mark it
                    while(!marked[0]){
                        nodeToRemove.next[level].compareAndSet(succ, succ, false, true);    // Mark the node
                        succ = nodeToRemove.next[level].get(marked);    // Update marked
                    }
                }
                boolean[] marked = {false}; // Holds the marked variable
                succ = nodeToRemove.next[bottomLevel].get(marked);

                // Keep trying to mark bottom level
                while(true){
                    boolean iMarkedIt = nodeToRemove.next[bottomLevel].compareAndSet(succ, succ, false, true);  // Attempt to mark at bottom level
                    succ = succs[bottomLevel].next[bottomLevel].get(marked);
                    // CAS was successful, call find (optimization) then return
                    if(iMarkedIt){
                        find(x, preds, succs);
                        return true;
                    }
                    // CAS was unsuccessful and the next ref was already marked - another thread removed - return false
                    else if(marked[0]) return false;
                }
            }
        }
    }

    // Node class for the skip list
    private static class Node{
        Integer value;
        int key;
        AtomicMarkableReference<Node>[] next;
        private int topLevel;

        // Constructor for sentinel nodes
        public Node(int key){
            value = null; this.key = key;
            next = new AtomicMarkableReference[MAX_LEVEL+1];
            for(int i = 0; i < next.length; i++) next[i] = new AtomicMarkableReference<Node>(null, false);
            topLevel = MAX_LEVEL;
        }

        // Constructor for normal Nodes
        public Node(int value, int height){
            this.value = value;
            this.key = value;
            next = new AtomicMarkableReference[height+1];
            for(int i = 0; i < next.length; i++) next[i] = new AtomicMarkableReference<Node>(null, false);
            topLevel = height;
        }
    }

    public void print(){
        for(int i = MAX_LEVEL; i >= 0; i--){
            AtomicMarkableReference<Node> curr = head.next[i];
            System.out.print(head.key + "\t");
            while(curr.getReference() != null) {
                System.out.print(curr.getReference().key + "\t");
                curr = curr.getReference().next[i];
            }
            System.out.println();
        }
    }

}
