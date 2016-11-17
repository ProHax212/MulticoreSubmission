import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Main program for testing the fine grained and lock based priority queues
 * numInserters - number of threads that will be adding to the priority queue
 * numInsert - how many elements each thread will add
 * numDeleters - number of threads that will be deleting from the priority queue
 * numDelete - number of times each thread will try to delete
 * There are 2 test methods, one for each implementation
 * The end of each test method will print out the resulting structure that was used followed by a boolean variable saying if the state of the structure is correct
 */
public class TestMain {

    public static void main(String[] args) {
        int numInserters = 10; int numDeleters = 10;
        int numInsert = 1000; int numDelete = 100;

        //fineGrainedTest(numInserters, numInsert, numDeleters, numDelete);
        lockFreeTest(numInserters, numInsert, numDeleters, numDelete);
    }

    private static void fineGrainedTest(int numInserters, int numInsert, int numDeleters, int numDelete){
        FineGrainedPriorityQueue fineGrainedPriorityQueue = new FineGrainedPriorityQueue();
        Random r = new Random();
        ExecutorService inserters = Executors.newFixedThreadPool(numInserters);
        ExecutorService deleters = Executors.newFixedThreadPool(numDeleters);

        for(int i = 0; i < numInserters; i++){
            inserters.submit(new Runnable() {
                @Override
                public void run() {
                    for(int i = 0; i < numInsert; i++){
                        int num = r.nextInt(10000);
                        fineGrainedPriorityQueue.insert(num, num);
                    }
                }
            });
        }

        for(int i = 0; i < numDeleters; i++){
            deleters.submit(new Runnable() {
                @Override
                public void run() {
                    for(int i = 0; i < numDelete; i++){
                        Integer num = fineGrainedPriorityQueue.deleteMin();
                        if(num != null) System.out.println(num);
                    }
                }
            });
        }

        try {
            inserters.shutdown();
            deleters.shutdown();
            inserters.awaitTermination(10, TimeUnit.SECONDS);
            deleters.awaitTermination(10, TimeUnit.SECONDS);
        }catch (InterruptedException e){

        }

        System.out.println(fineGrainedPriorityQueue);
        System.out.println("VALID_STATE: " + fineGrainedPriorityQueue.verify());
    }

    private static void lockFreeTest(int numInserters, int numInsert, int numDeleters, int numDelete){
        LockFreePriorityQueue lockFreePriorityQueue = new LockFreePriorityQueue();
        Random r = new Random();
        ExecutorService inserters = Executors.newFixedThreadPool(numInserters);
        ExecutorService deleters = Executors.newFixedThreadPool(numDeleters);

        for (int i = 0; i < numInserters; i++) {
            inserters.execute(new Runnable() {
                @Override
                public void run() {
                    for(int i = 0; i < numInsert; i++){
                        int num = r.nextInt(10000);
                        lockFreePriorityQueue.insert(num, num);
                    }
                }
            });
        }

        for (int i = 0; i < numDeleters; i++) {
            deleters.execute(new Runnable() {
                @Override
                public void run() {
                    for(int i = 0; i < numDelete; i++){
                        Integer num = lockFreePriorityQueue.deleteMin();
                        if(num != null) System.out.println(num);
                    }
                }
            });
        }

        try{
            inserters.shutdown();
            deleters.shutdown();
            inserters.awaitTermination(10, TimeUnit.SECONDS);
            deleters.awaitTermination(10, TimeUnit.SECONDS);
        }catch (InterruptedException e){}

        System.out.println(lockFreePriorityQueue);
        System.out.println("VALID_STATE: " + lockFreePriorityQueue.verify());
    }

}
