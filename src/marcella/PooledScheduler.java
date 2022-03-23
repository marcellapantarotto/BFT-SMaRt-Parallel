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
import parallelism.late.ConflictDefinition;
import parallelism.scheduler.Scheduler;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static com.codahale.metrics.MetricRegistry.name;
import static jdk.nashorn.internal.objects.NativeArray.forEach;
import parallelism.MultiOperationCtx;

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
    private final Stats stats;
    private final ExecutorService pool;

    private Consumer<MessageContextPair> executor;

    PooledScheduler(int nThreads,
            ConflictDefinition conflict,
            MetricRegistry metrics) {
        this.nThreads = nThreads;
        this.conflict = conflict;
        this.space = new Semaphore(MAX_SIZE);
        this.scheduled = new LinkedList<>();
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
        try {
            space.acquire();
            stats.size.inc();       // increment job, which is size of stats
            doSchedule(request);
        } catch (InterruptedException e) {
            // Ignored.
        }
    }

    public void schedule(TOMMessage request) {
        MultiOperationRequest reqs = new MultiOperationRequest(request.getContent());
        MultiOperationCtx ctx = new MultiOperationCtx(reqs.operations.length, request);
        for (int i = 0; i < reqs.operations.length; i++) {
            this.schedule(new MessageContextPair(request, request.groupId, i, reqs.operations[i], reqs.opId, ctx));
        }
    }

    // schedualing task in list //***TO DO
    private void doSchedule(MessageContextPair request) {
        Task newTask = new Task(request);
        submit(newTask, addTask(newTask));
    }

    // adding task to list
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

    @Override
    public ParallelMapping getMapping() {
        return null;
    }

    @Override
    public void scheduleReplicaReconfiguration() {
    }
}
