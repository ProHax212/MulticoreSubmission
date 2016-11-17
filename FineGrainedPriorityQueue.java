import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A fine grained lock based priority queue
 * The algorithm that was used was "An efficient algorithm for concurrent priority queue heaps"
 * Authors: "Galen C. Hunt, Maged M. Michael, Srinivasan Parthasarathy, Michael L. Scott"
 * Link - http://www.research.ibm.com/people/m/michael/ipl-1996.pdf
 */
public class FineGrainedPriorityQueue {

    // Maximum length of the heap
    // If this is too high and a lot of inserts can occur concurrently, then starvation could happen
    private static final int MAX_LENGTH = 100;

    private Node[] heap;
    private ReentrantLock heapLock;
    int nextIndex;

    // Constructor for the fine grained priority queue
    // Initialize every node to empty
    public FineGrainedPriorityQueue(){
        heap = new Node[MAX_LENGTH+1];
        for(int i = 0; i < MAX_LENGTH+1; i++){
            heap[i] = new Node();
        }
        nextIndex = 1;
        heapLock = new ReentrantLock();
    }

    // Insert a new node into the priority queue
    public boolean insert(Integer value, int priority){
        // Temporarily lock the heap while adding
        heapLock.lock();
        int index = nextIndex;

        // Not enough room
        if((index*2 + 1) >= heap.length){
            heapLock.unlock();
            return false;
        }

        nextIndex += 1;
        heap[index].lock(); heapLock.unlock();
        heap[index].priority =  priority;
        heap[index].tag = Thread.currentThread().getId();
        heap[index].unlock();

        // Percolate up while priority is higher than parent
        boolean Done = false;
        while(index > 1 && !Done){
            int parent = index/2;
            int last = index;
            heap[parent].lock();
            heap[index].lock();

            // Parent is available and the current node is tagged by me
            if(heap[parent].tag == -1L && heap[index].tag == Thread.currentThread().getId()){
                // Parent has lower priority - swap them
                if(heap[parent].priority > heap[index].priority){
                    swapNodes(heap[parent], heap[index]);
                    index = parent;
                }
                // Done percolating up
                else{
                    heap[index].tag = -1L; Done = true;
                }
            }
            // Tag of the parent is EMPTY (the current node is now at the root)
            else if(heap[parent].tag == -2L){
                Done = true;
            }
            // Tag of the current node is NOT my process ID -> have to chase it up the heap
            else if(heap[index].tag != Thread.currentThread().getId()){
                index = parent;
            }
            heap[last].unlock();
            heap[parent].unlock();
        }

        // First insert
        if(index == 1){
            heap[1].lock();
            if(heap[1].tag == Thread.currentThread().getId()) heap[1].tag = -1L;  // Available
            heap[1].unlock();
        }

        return true;
    }

    // Remove the highest priority node from the priority queue
    public Integer deleteMin(){
        int child;
        // Get the data off the root node then delete it
        heapLock.lock();
        int index = (nextIndex - 1);

        // Heap is empty - return null
        if(index == 0){
            heapLock.unlock();
            return null;
        }

        nextIndex -= 1;
        heap[1].lock(); heap[index].lock(); heapLock.unlock();
        int priority = heap[1].priority;
        heap[1].tag = -2L;

        // Swap priorities
        swapNodes(heap[1], heap[index]);
        heap[index].unlock();

        // Stop if its the only item in heap
        if(heap[1].tag == -2L){
            heap[1].unlock();
            return priority;
        }

        heap[1].tag = -1L;

        // Start percolating down
        index = 1;
        while(index < heap.length/2) {
            int left = index * 2, right = index * 2 + 1;
            heap[left].lock();
            heap[right].lock();

            // No left child - done
            if(heap[left].tag == -2L){
                heap[right].unlock(); heap[left].unlock();
                break;
            }
            // No right child or left has higher priority than right
            else if((heap[right].tag == -2L) || (heap[left].priority < heap[right].priority)){
                heap[right].unlock();
                child = left;
            }
            // Right child has higher priority
            else{
                heap[left].unlock();
                child = right;
            }

            // If child has higher priority, then swap
            if((heap[child].priority < heap[index].priority) && (heap[child].tag != -2L)){
                swapNodes(heap[child], heap[index]);
                heap[index].unlock();
                index = child;
            }else{
                heap[child].unlock();
                break;
            }
        }
        heap[index].unlock();

        return priority;
    }

    // Swap the values of the two nodes
    private void swapNodes(Node one, Node two){
        Node temp = new Node(one.value, one.priority, one.tag);

        one.value = two.value;
        one.priority = two.priority;
        one.tag = two.tag;

        two.value = temp.value;
        two.priority = temp.priority;
        two.tag = temp.tag;
    }

    // Private node class
    private static class Node{

        Integer value;
        int priority;
        long tag;   // -2 (EMPTY), -1 (AVAILABLE), threadID (being inserted)
        ReentrantLock lock;

        public Node(Integer value, int priority, long threadId){
            this.value = value;
            this.priority = priority;
            this.tag = threadId;
            this.lock = new ReentrantLock();
        }

        public Node(){
            tag = -2L;
            lock = new ReentrantLock();
        }

        public void lock(){
            this.lock.lock();
        }

        public void unlock(){
            this.lock.unlock();
        }

    }

    // Verify the state of the Heap
    // Every parent node should have a lower key than its children
    public boolean verify(){
        for(int i = 1; i < (nextIndex - 1)/2; i++){
            int parent = i/2, left = 2*i, right = 2*i + 1;

            if((heap[parent].priority > heap[i].priority) && parent>=1) return false;
            if(heap[left].priority < heap[i].priority) return false;
            if(heap[right].priority < heap[i].priority) return false;
        }

        return true;
    }

    // Tostring method for the heap
    // Simply print the array
    public String toString(){
        String returnString = "";
        for(Node node : heap){
            if(node.tag != -2L) returnString += node.priority + ", ";
            else returnString += "EMPTY, ";
        }

        return returnString;
    }

}
