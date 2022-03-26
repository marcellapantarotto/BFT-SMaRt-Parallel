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
import parallelism.MultiOperationCtx;

final class PooledScheduler implements Scheduler {

    public static boolean normal = false;

    private static final int MAX_SIZE = 150;

    private static final class Task {

        private final MessageContextPair request;
        private final CompletableFuture<Void> future;

        Task(MessageContextPair request) {
            this.request = request;
            this.future = new CompletableFuture<>();
        }
    }

    private static class Stats {

        final Counter size;
        final Counter ready;

        Stats(MetricRegistry metrics) {
            size = metrics.counter(name(PooledScheduler.class, "size"));
            ready = metrics.counter(name(PooledScheduler.class, "ready"));
        }
    }

    private final int nThreads;
    private final ConflictDefinition conflict;
    private final ExecutorService pool;
    private final Semaphore space;
    private final List<Task> scheduled;
    private final Stats stats;

    private Consumer<MessageContextPair> executor;

    private MessageContextPair primeira = null;
    private MessageContextPair ultima = null;
    private int count = 0;

    PooledScheduler(int nThreads,
            ConflictDefinition conflict,
            MetricRegistry metrics) {
        this.nThreads = nThreads;
        this.conflict = conflict;
        this.space = new Semaphore(MAX_SIZE);
        this.scheduled = new LinkedList<>();
        this.stats = new Stats(metrics);
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
        // R1 = 11; R8 = 18; GR = 31; R12 = 112; R78 = 178
        if (normal || (11 <= request.opId && request.opId <= 18)
                || request.opId == 31
                || (112 <= request.opId && request.opId < 178)) {
            //escalona
            try {
                space.acquire();
                stats.size.inc();       // increment job, which is size of stats
                doSchedule(request);
            } catch (InterruptedException e) {
                // Ignored.
            }

        } else {//conflito

            if (primeira == null) {
                primeira = request;
                ultima = request;
            } else {
                ultima.next = request;
                ultima = request;
            }
            count++;
            if (count == 200) {
                //escalona
                try {
                    space.acquire();
                    stats.size.inc();
                    // increment job, which is size of stats
                    //System.out.println("ENTROU AQUI");
                    doSchedule(primeira);
                } catch (InterruptedException e) {
                    // Ignored.
                }
                primeira = null;
                ultima = null;
                count = 0;
            }
        }

    }

    public void schedule(TOMMessage request) {
        MultiOperationRequest reqs = new MultiOperationRequest(request.getContent());
        MultiOperationCtx ctx = new MultiOperationCtx(reqs.id1.length, request);
        for (int i = 0; i < reqs.id1.length; i++) {
            this.schedule(new MessageContextPair(request, request.groupId, i, reqs.id1[i], reqs.id1[i], reqs.opId, ctx));
        }
    }

    private void doSchedule(MessageContextPair request) {
        Task newTask = new Task(request);
        submit(newTask, addTask(newTask));
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

    private void submit(Task newTask, List<CompletableFuture<Void>> dependencies) {
        if (dependencies.isEmpty()) {
            stats.ready.inc();
            pool.execute(() -> execute(newTask));
        } else {
            after(dependencies).thenRun(() -> {
                stats.ready.inc();
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

        executor.accept(task.request);
        space.release();
        stats.ready.dec();
        stats.size.dec();
        task.future.complete(null);
    }

    @Override
    public ParallelMapping getMapping() {
        return null;
    }

    @Override
    public void scheduleReplicaReconfiguration() {
    }
}
