/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.late;

import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.server.Executable;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.util.ThroughputStatistics;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.concurrent.CyclicBarrier;
import parallelism.MessageContextPair;
import parallelism.MultiOperationCtx;
import parallelism.ParallelMapping;
import parallelism.ParallelServiceReplica;
import parallelism.late.graph.DependencyGraph;

/**
 *
 * @author eduardo
 */
public class LateServiceReplica extends ParallelServiceReplica {

    private CyclicBarrier recBarrier = new CyclicBarrier(2);

    public LateServiceReplica(int id, Executable executor, Recoverable recoverer, int numWorkers, ConflictDefinition cf, COSType graphType, int partitions) {
        super(id, executor, recoverer, new LateScheduler(cf, numWorkers, graphType));
        String path = "resultsLockFree_" + id + "_" + partitions + "_" + numWorkers + ".txt";
        statistics = new ThroughputStatistics(id, numWorkers, path, "late");
    }

    public CyclicBarrier getReconfBarrier() {
        return recBarrier;
    }

    @Override
    public int getNumActiveThreads() {
        return this.scheduler.getNumWorkers();
    }

    @Override
    protected void initWorkers(int n, int id) {

        //statistics = new ThroughputStatistics(id, n, "results_" + id + ".txt", "");
        int tid = 0;
        for (int i = 0; i < n; i++) {
            new LateServiceReplicaWorker((LateScheduler) this.scheduler, tid).start();
            tid++;
        }
    }

    private class LateServiceReplicaWorker extends Thread {

        private LateScheduler s;
        private int thread_id;

        public LateServiceReplicaWorker(LateScheduler s, int id) {
            this.thread_id = id;
            this.s = s;

            //System.out.println("Criou um thread: " + id);
        }
        //int exec = 0;

        public byte[] serialize(short opId, short value) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream oos = new DataOutputStream(baos);

                oos.writeShort(opId);

                oos.writeShort(value);

                oos.close();
                return baos.toByteArray();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        public void run() {
            //System.out.println("rum: " + thread_id);
            MessageContextPair msg = null;
            while (true) {
                // System.out.println("vai pegar...");
                Object node = s.get();
                //System.out.println("Pegou req...");

                msg = ((DependencyGraph.vNode) node).getAsRequest();
                //System.out.println("Pegou req..."+ msg.toString());

                for (int i = 0; i < msg.request.size(); i++) {
                    if (msg.classId.get(i) == (int) ParallelMapping.CONFLICT_RECONFIGURATION) {
                        try {
                            getReconfBarrier().await();
                            getReconfBarrier().await();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    } else {
                        //msg.resp = ((SingleExecutable) executor).executeOrdered( msg.operation, null);
                        //exec++;
                        msg.resp.add(((SingleExecutable) executor).executeOrdered(serialize(msg.opId.get(i), msg.operation.get(i)), null));

                        //System.out.println(thread_id+" Executadas: "+exec);
                        //MultiOperationCtx ctx = ctxs.get(msg.request.toString());
                        msg.ctx.get(i).add(msg.index.get(i), msg.resp.get(i));
                        if (msg.ctx.get(i).response.isComplete() && !msg.ctx.get(i).finished && (msg.ctx.get(i).interger.getAndIncrement() == 0)) {
                            msg.ctx.get(i).finished = true;
                            msg.ctx.get(i).request.reply = new TOMMessage(id, msg.ctx.get(i).request.getSession(),
                                    msg.ctx.get(i).request.getSequence(), msg.ctx.get(i).response.serialize(), SVController.getCurrentViewId());
                            //bftsmart.tom.util.Logger.println("(ParallelServiceReplica.receiveMessages) sending reply to "
                            //      + msg.message.getSender());
                            //TODO:only for local execution
                            replier.manageReply(msg.ctx.get(i).request, null);
                        }
                        statistics.computeStatistics(thread_id, 1);
                    }
                }

                //s.removeRequest(msg);
                s.remove(node);
            }
        }
    }
}
