/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marcella;

import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.util.MultiOperationRequest;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import parallelism.MessageContextPair;
import parallelism.ParallelMapping;
import parallelism.MultiOperationCtx;
import parallelism.late.ConflictDefinition;
import parallelism.scheduler.Scheduler;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.ArrayList;

import static com.codahale.metrics.MetricRegistry.name;

final class PooledScheduler implements Scheduler {

    private static final int MAX_SIZE = 150;

    private static final class Task {

        private final MessageContextPair request;
        private final CompletableFuture<Void> future;   // tarefa

        Task(MessageContextPair request) {
            this.request = request;
            this.future = new CompletableFuture<>();
        }
    }

    private static class Stats {

        // A counter is a simple incrementing and decrementing 64-bit integer
        final Counter size;
        final Counter ready;

        Stats(MetricRegistry metrics) {
            // Static method to construct names of counter types
            size = metrics.counter(name(PooledScheduler.class, "size"));
            ready = metrics.counter(name(PooledScheduler.class, "ready"));
        }
    }

    private final int nThreads;
    private final ConflictDefinition conflict;
    private final Semaphore space;
    private final List<Task> scheduled;
    private final List<List<Task>> scheduledList;
    private final Stats stats;
    private final ExecutorService pool;

    private Consumer<MessageContextPair> executor;

    private final List<TOMMessage> requestList = new ArrayList<TOMMessage>();
    private final List<MultiOperationRequest> reqsList = new ArrayList<>();
    private final List<MultiOperationCtx> ctxList = new ArrayList<>();
    private final List<Integer> indexList = new ArrayList<>();
    private final List<Integer> groupIdList = new ArrayList<>();
    private final List<Short> operationList = new ArrayList<>();
    private final List<Short> opIdList = new ArrayList<>();

    PooledScheduler(int nThreads,
            ConflictDefinition conflict,
            MetricRegistry metrics) {
        this.nThreads = nThreads;
        this.conflict = conflict;
        this.space = new Semaphore(MAX_SIZE);
        this.scheduled = new LinkedList<>();
        this.scheduledList = new LinkedList<>();
        this.stats = new Stats(metrics);        // new metric instance
        this.pool = new ForkJoinPool(
                nThreads, ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null, true, nThreads, nThreads, 0, null, 60, TimeUnit.SECONDS);
    }

    // Breaks cyclic dependency with PooledServiceReplica
    void setExecutor(Consumer<MessageContextPair> executor) {
        this.executor = executor;
    }

    @Override
    public int getNumWorkers() {
        return nThreads;
    }

    @Override
    public void schedule(MessageContextPair request) {
        // linked list to add conflicting requests
        LinkedList<MessageContextPair> conflictingRequests = new LinkedList();

        for (int i = 0; i < request.opId.size(); i++) {
            // R1 = 11; R8 = 18; GR = 31; R12 = 112; R78 = 178
            if ((11 <= request.opId.get(i) && request.opId.get(i) <= 18)
                    || request.opId.get(i) == 31
                    || (112 <= request.opId.get(i) && request.opId.get(i) < 178)) {
                //escalona
                try {
                    space.acquire();
                    stats.size.inc();       // increment job, which is size of stats
                    doSchedule(request);
                } catch (InterruptedException e) {
                    // Ignored.
                }
            } else {
                // adds request to linked list
                conflictingRequests.add(request);
                // add whole list to node
                if (conflictingRequests.size() == MAX_SIZE) {
                    try {
                        space.acquire(MAX_SIZE);
                        stats.size.inc(MAX_SIZE);       // increment job, which is size of stats
                        doScheduleList(conflictingRequests);
                    } catch (InterruptedException e) {
                        // Ignored.
                    }
                }
                conflictingRequests.clear();
            }
        }

///////////////////////////////////////////////////////////////////////////////////////////////////
//        try {
//                    space.acquire();
//                    stats.size.inc();       // increment job, which is size of stats
//                    doSchedule(request);
//                } catch (InterruptedException e) {
//                    // Ignored.
//                }
///////////////////////////////////////////////////////////////////////////////////////////////////
    }

    @Override
    public void schedule(TOMMessage request) {
        requestList.add(request);
        reqsList.add(new MultiOperationRequest(request.getContent()));

        if (requestList.size() == MAX_SIZE) {
            for (int i = 0; i < requestList.size(); i++) {
                ctxList.add(new MultiOperationCtx(reqsList.get(i).operations.length, request));
                groupIdList.add(requestList.get(i).groupId);
                opIdList.add(reqsList.get(i).opId);

                for (int j = 0; j < reqsList.get(i).operations.length; j++) {
                    indexList.add(j);
                    operationList.add(reqsList.get(i).operations[j]);
                }

                this.schedule(new MessageContextPair(requestList, groupIdList, indexList, operationList, opIdList, ctxList));
            }

            requestList.clear();
            groupIdList.clear();
            indexList.clear();
            operationList.clear();
            opIdList.clear();
            ctxList.clear();
            reqsList.clear();
        }
        
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//        MultiOperationRequest reqs = new MultiOperationRequest(request.getContent());
//        MultiOperationCtx ctx = new MultiOperationCtx(reqs.operations.length, request);
//        for (int i = 0; i < reqs.operations.length; i++) {
//            this.schedule(new MessageContextPair(request, request.groupId, i, reqs.operations[i], reqs.opId, ctx));
//        }
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    }
    
    // schedualing one request
    private void doSchedule(MessageContextPair request) {
        Task newTask = new Task(request);
        submit(newTask, addTask(newTask));
    }

    // schedualing list os requests
    private void doScheduleList(LinkedList<MessageContextPair> requests) {
        LinkedList<Task> listTasks = new LinkedList();
        for (MessageContextPair r : requests) {
            Task newTask = new Task(r);
            listTasks.add(newTask);
        }
        submitList(listTasks, addListTask(listTasks));
    }

    private List<CompletableFuture<Void>> addTask(Task newTask) {
        List<CompletableFuture<Void>> dependencies = new LinkedList<>();
        ListIterator<Task> iterator = scheduled.listIterator();

        while (iterator.hasNext()) {
            Task task = iterator.next();
            if (task.future.isDone()) {
                iterator.remove();
                continue;
            }
            if (conflict.isDependent(task.request, newTask.request)) {
                dependencies.add(task.future);
            }
        }

        scheduled.add(newTask);
        return dependencies;
    }

    // adding task to list
    private List<List<CompletableFuture<Void>>> addListTask(LinkedList<Task> listTasks) {
        List<List<CompletableFuture<Void>>> listDependencies = new LinkedList<>();

        for (int i = 0; i < listTasks.size(); i++) {
            List<CompletableFuture<Void>> dependencies = new LinkedList<>();
            ListIterator<List<Task>> iterator = scheduledList.listIterator();

            while (iterator.hasNext()) {
                List<Task> listTask = iterator.next();
                ListIterator<Task> iteratorTask = listTask.listIterator();

                while (iteratorTask.hasNext()) {
                    Task task = iteratorTask.next();
                    if (task.future.isDone()) {
                        iteratorTask.remove();
                        continue;
                    }
                    if (conflict.isDependent(task.request, listTasks.get(i).request)) {
                        dependencies.add(task.future);
                    }
                }
                iterator.remove();
                continue;
            }
            listDependencies.add(dependencies);
        }
        scheduledList.add(listTasks);
        return listDependencies;
    }

    private void submit(Task newTask, List<CompletableFuture<Void>> dependencies) {
        if (dependencies.isEmpty()) {
            stats.ready.inc();       // increment job, which is if stats is ready
            pool.execute(() -> execute(newTask));   // submitting task to ForkJoinPool
        } else {
            after(dependencies).thenRun(() -> {
                stats.ready.inc();       // increment job, which is if stats is ready
                execute(newTask);
            });
        }
    }

    private void submitList(LinkedList<Task> listTasks, List<List<CompletableFuture<Void>>> dependencies) {
        for (int i = 0; i < listTasks.size(); i++) {
            if (dependencies.get(i).isEmpty()) {
                stats.ready.inc();       // increment job, which is if stats is ready
                pool.execute(() -> executeList(listTasks));   // submitting task to ForkJoinPool
            } else {
                after(dependencies.get(i)).thenRun(() -> {
                    stats.ready.inc();       // increment job, which is if stats is ready
                    executeList(listTasks);
                });
            }
        }
    }

    private static CompletableFuture<Void> after(List<CompletableFuture<Void>> fs) {
        if (fs.size() == 1) {
            return fs.get(0); // fast path
        }
        return CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]));
    }

    private void execute(Task task) {
        executor.accept(task.request);  // executar requisição
        space.release();
        stats.ready.dec();       // decrement job, which is if stats is ready
        stats.size.dec();        // decrement job, which is size of stats
        task.future.complete(null); // liberar as outras que dependem dessa tarefa
    }

    private void executeList(LinkedList<Task> task) {
        for (int i = 0; i < task.size(); i++) {
            executor.accept(task.get(i).request);  // executar requisição
            space.release();
            stats.ready.dec();       // decrement job, which is if stats is ready
            stats.size.dec();        // decrement job, which is size of stats
            task.get(i).future.complete(null); // liberar as outras que dependem dessa tarefa
        }
    }

    @Override
    public ParallelMapping getMapping() {
        return null;
    }

    @Override
    public void scheduleReplicaReconfiguration() {
    }
}
