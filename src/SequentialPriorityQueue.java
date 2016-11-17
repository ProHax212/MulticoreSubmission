import java.util.ArrayList;

/**
 * Sequential version of a priority queue
 * Implement this first to get an idea of the functionality
 * Implement as a max heap
 */
public class SequentialPriorityQueue {

    ArrayList<Node> heap;

    // Constructor
    public SequentialPriorityQueue(){
        heap = new ArrayList<>();
        heap.add(new Node(42, 1));   // Sentinel Node
    }

    // Insert an object with the given priority
    public void insert(Integer value, int priority){
        // Is the queue empty
        if(heap.size() == 0){
            heap.add(new Node(value, priority));
            return;
        }

        // Insert at the end and percolate up
        Node newNode = new Node(value, priority);
        heap.add(newNode);
        int index = heap.size() - 1;
        int parentIndex = getParent(index);
        // Loop while parent isn't null and has a lower priority
        while(index != 0 && heap.get(parentIndex).priority < priority){
            // Swap with parent
            Node temp = heap.get(parentIndex);
            heap.set(parentIndex, newNode);
            heap.set(index, temp);

            // Update indecies
            index = parentIndex;
            parentIndex = getParent(index);
        }
    }

    // Take the top object off (highest priority)
    public Integer remove(){
        // Check if the queue is empty
        if(heap.size() == 0) return null;   // Or throw an exception

        // Swap the head with the last element and remove
        Node returnNode = heap.get(0);
        heap.set(0, heap.get(heap.size()-1));
        heap.remove(heap.size()-1);

        // Percolate the new head down
        int index = 0;
        int leftChildIndex = getLeftChild(0);
        int rightChildIndex = getRightChild(0);
        System.out.println("Index: " + index + "\tLeft: " + leftChildIndex + "\tRight: " + rightChildIndex);
        // Loop while at least one of the children has higher priority
        while(heap.get(leftChildIndex).priority > heap.get(index).priority || heap.get(rightChildIndex).priority > heap.get(index).priority){
            // Left child has higher priority
            if(heap.get(leftChildIndex).priority > heap.get(rightChildIndex).priority){
                Node temp = heap.get(leftChildIndex);
                heap.set(leftChildIndex, heap.get(index));
                heap.set(index, temp);
                index = leftChildIndex;
            }else if(heap.get(rightChildIndex).priority > heap.get(leftChildIndex).priority){   // Right child has higher priority
                Node temp = heap.get(rightChildIndex);
                heap.set(rightChildIndex, heap.get(index));
                heap.set(index, temp);
                index = rightChildIndex;
            }else{  // Same priority - swap with left
                Node temp = heap.get(leftChildIndex);
                heap.set(leftChildIndex, heap.get(index));
                heap.set(index, temp);
                index = leftChildIndex;
            }

            // Update left and right child indecies
            leftChildIndex = getLeftChild(index);
            rightChildIndex = getRightChild(index);
        }

        return returnNode.value;
    }

    // Node for each element of the priority queue
    private static class Node{

        public int priority;
        public Integer value;

        // Constructor
        public Node(Integer value, int priority){
            this.priority = priority;
            this.value = value;
        }

    }

    // Helper methods for indexing the heap
    private int getParent(int index){return index/2;}
    private int getLeftChild(int index){return 2*index;}
    private int getRightChild(int index){return 2*index+1;}

    public String toString(){
        String returnString = "";
        for(int i = 0; i < heap.size(); i++){
            returnString += heap.get(i).value + ", ";
        }

        return returnString;
    }

}
