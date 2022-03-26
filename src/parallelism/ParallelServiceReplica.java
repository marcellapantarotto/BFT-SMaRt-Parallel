/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism;

import bftsmart.consensus.messages.MessageFactory;
import bftsmart.consensus.roles.Acceptor;
import bftsmart.consensus.roles.Proposer;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.ExecutionManager;
import bftsmart.tom.core.ParallelTOMLayer;
import bftsmart.tom.core.messages.TOMMessage;

import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.server.Executable;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.tom.util.ShutdownHookThread;
import bftsmart.tom.util.TOMUtil;
import bftsmart.util.MultiOperationRequest;
import bftsmart.util.ThroughputStatistics;
import demo.list.ListClientMO;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;
import parallelism.hybrid.early.TOMMessageWrapper;
import parallelism.scheduler.DefaultScheduler;
import parallelism.scheduler.Scheduler;

/**
 *
 * @author alchieri
 */
public class ParallelServiceReplica extends ServiceReplica {

    protected Scheduler scheduler;
    public ThroughputStatistics statistics;

    private int partitions = 1;

    //protected Map<String, MultiOperationCtx> ctxs = new Hashtable<>();
    public ParallelServiceReplica(int id, Executable executor, Recoverable recoverer, int initialWorkers) {
        //this(id, executor, recoverer, new DefaultScheduler(initialWorkers));
        super(id, executor, recoverer);

        createScheduler(initialWorkers);

        initWorkers(this.scheduler.getNumWorkers(), id);

    }

    public ParallelServiceReplica(int id, Executable executor, Recoverable recoverer, Scheduler s) {
        //this(id, executor, recoverer, new DefaultScheduler(initialWorkers));
        super(id, executor, recoverer);

        this.scheduler = s;
        initWorkers(this.scheduler.getNumWorkers(), id);
    }

    public ParallelServiceReplica(int id, Executable executor, Recoverable recoverer, int initialWorkers, int partitions, ClassToThreads[] cts) {
        //this(id, executor, recoverer, new DefaultScheduler(initialWorkers));
        super(id, executor, recoverer);

        this.partitions = partitions;
        if (initialWorkers <= 0) {
            initialWorkers = 1;
        }

        this.scheduler = new DefaultScheduler(initialWorkers, cts);

        initWorkers(this.scheduler.getNumWorkers(), id);
    }

    protected void createScheduler(int initialWorkers) {
        if (initialWorkers <= 0) {
            initialWorkers = 1;
        }
        int[] ids = new int[initialWorkers];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = i;
        }
        ClassToThreads[] cts = new ClassToThreads[2];
        cts[0] = new ClassToThreads(ParallelMapping.CONC_ALL, ClassToThreads.CONC, ids);
        cts[1] = new ClassToThreads(ParallelMapping.SYNC_ALL, ClassToThreads.SYNC, ids);
        this.scheduler = new DefaultScheduler(initialWorkers, cts);

    }


    /* public ParallelServiceReplica(int id, Executable executor, Recoverable recoverer, int minWorkers, int initialWorkers, int maxWorkers) {
        this(id, executor, recoverer, new ReconfigurableScheduler(minWorkers, initialWorkers, maxWorkers, null, id));
    }

    public ParallelServiceReplica(int id, Executable executor, Recoverable recoverer,
            int minWorkers, int initialWorkers, int maxWorkers, PSMRReconfigurationPolicy rec) {
        this(id, executor, recoverer, new ReconfigurableScheduler(minWorkers, initialWorkers, maxWorkers, rec, id));
    }*/
 /*public ParallelServiceReplica(int id, Executable executor, Recoverable recoverer, Scheduler s) {
        super(id, executor, recoverer);
        //this.mapping = m;
        if (s == null) {
            this.scheduler = new DefaultScheduler(1);
        } else {
            this.scheduler = s;
        }

        //this.finished = new FIFOQueue<>();
        //new SenderWorker(finished).start();
        initWorkers(this.scheduler.getNumWorkers(), id);
    }*/
    protected void initWorkers(int n, int id) {
        /* if(this.scheduler.getMapping().getNumThreadsAC() <= 1){
            useBarrier = false;
        }*/

        // statistics = new ThroughputStatistics(id, n, "early" + id + ".txt", "");
        String path = "resultsEarly_" + id + "_" + this.partitions + "_" + n + ".txt";
        statistics = new ThroughputStatistics(id, n, path, "");

        int tid = 0;
        for (int i = 0; i < n; i++) {
            new ServiceReplicaWorker(this.scheduler.getMapping().getAllQueues()[i], tid).start();
            tid++;
        }
    }

    /* public ParallelServiceReplica(int id, boolean isToJoin, Executable executor, Recoverable recoverer, ParallelMapping m, PSMRReconfigurationPolicy rp) {
        super(id, "", isToJoin, executor, recoverer);
        this.mapping = m;
        //alexend
        if (rp == null) {
            this.reconf = new DefaultPSMRReconfigurationPolicy();
        } else {
            this.reconf = rp;
        }
        //alexend
        initMapping();
    }*/
    public int getNumActiveThreads() {
        return this.scheduler.getMapping().getNumWorkers();
    }

    /*public boolean addExecutionConflictGroup(int groupId, int[] threadsId) {
        return this.scheduler.getMapping().addMultiGroup(groupId, threadsId);
    }*/
    /**
     * Barrier used to reconfigure the number of replicas in the system
     *
     * @return
     */
    public CyclicBarrier getReconfBarrier() {
        return this.scheduler.getMapping().getReconfBarrier();
    }

    public void localExecution(TOMMessage req) {
        statistics.start();
        this.scheduler.schedule(req);
    }

    @Override
    public void receiveMessages(int consId[], int regencies[], int leaders[], CertifiedDecision[] cDecs, TOMMessage[][] requests) {

        //int numRequests = 0;
        int consensusCount = 0;
        boolean noop = true;

        for (TOMMessage[] requestsFromConsensus : requests) {
            TOMMessage firstRequest = requestsFromConsensus[0];
            //int requestCount = 0;
            noop = true;
            for (TOMMessage request : requestsFromConsensus) {

                bftsmart.tom.util.Logger.println("(ServiceReplica.receiveMessages) Processing TOMMessage from client " + request.getSender() + " with sequence number " + request.getSequence() + " for session " + request.getSession() + " decided in consensus " + consId[consensusCount]);

                if (request.getViewID() == SVController.getCurrentViewId()) {
                    if (request.getReqType() == TOMMessageType.ORDERED_REQUEST) {
                        noop = false;
                        //numRequests++;
                        /*MessageContext msgCtx = new MessageContext(request.getSender(), request.getViewID(),
                                request.getReqType(), request.getSession(), request.getSequence(), request.getOperationId(),
                                request.getReplyServer(), request.serializedMessageSignature, firstRequest.timestamp,
                                request.numOfNonces, request.seed, regencies[consensusCount], leaders[consensusCount],
                                consId[consensusCount], cDecs[consensusCount].getConsMessages(), firstRequest, false);

                        if (requestCount + 1 == requestsFromConsensus.length) {

                            msgCtx.setLastInBatch();
                        }
                        request.deliveryTime = System.nanoTime();
                         */

                        //MultiOperationRequest reqs = new MultiOperationRequest(request.getContent());
                        // MultiOperationCtx ctx = new MultiOperationCtx(reqs.operations.length, request);
                        //this.ctxs.put(request.toString(), ctx);
                        statistics.start();
                        this.scheduler.schedule(request);
                        /*for (int i = 0; i < reqs.operations.length; i++) {
                            this.scheduler.schedule(new MessageContextPair(request, request.groupId, i, reqs.operations[i], reqs.opId));
                           
                        }*/

                    } else if (request.getReqType() == TOMMessageType.RECONFIG) {

                        SVController.enqueueUpdate(request);
                    } else {
                        throw new RuntimeException("Should never reach here! ");
                    }

                } else if (request.getViewID() < SVController.getCurrentViewId()) {
                    // message sender had an old view, resend the message to
                    // him (but only if it came from consensus an not state transfer)
                    tomLayer.getCommunication().send(new int[]{request.getSender()}, new TOMMessage(SVController.getStaticConf().getProcessId(),
                            request.getSession(), request.getSequence(), TOMUtil.getBytes(SVController.getCurrentView()), SVController.getCurrentViewId()));

                }
                //requestCount++;
            }

            //System.out.println("BATCH SIZE: "+requestCount);
            // This happens when a consensus finishes but there are no requests to deliver
            // to the application. This can happen if a reconfiguration is issued and is the only
            // operation contained in the batch. The recoverer must be notified about this,
            // hence the invocation of "noop"
            if (noop && this.recoverer != null) {

                bftsmart.tom.util.Logger.println("(ServiceReplica.receiveMessages) Delivering a no-op to the recoverer");

                System.out.println(" --- A consensus instance finished, but there were no commands to deliver to the application.");
                System.out.println(" --- Notifying recoverable about a blank consensus.");

                byte[][] batch = null;
                MessageContext[] msgCtx = null;
                if (requestsFromConsensus.length > 0) {
                    //Make new batch to deliver
                    batch = new byte[requestsFromConsensus.length][];
                    msgCtx = new MessageContext[requestsFromConsensus.length];

                    //Put messages in the batch
                    int line = 0;
                    for (TOMMessage m : requestsFromConsensus) {
                        batch[line] = m.getContent();

                        msgCtx[line] = new MessageContext(m.getSender(), m.getViewID(),
                                m.getReqType(), m.getSession(), m.getSequence(), m.getOperationId(),
                                m.getReplyServer(), m.serializedMessageSignature, firstRequest.timestamp,
                                m.numOfNonces, m.seed, regencies[consensusCount], leaders[consensusCount],
                                consId[consensusCount], cDecs[consensusCount].getConsMessages(), firstRequest, true);
                        msgCtx[line].setLastInBatch();

                        line++;
                    }
                }

                this.recoverer.noOp(consId[consensusCount], batch, msgCtx);

                //MessageContext msgCtx = new MessageContext(-1, -1, null, -1, -1, -1, -1, null, // Since it is a noop, there is no need to pass info about the client...
                //        -1, 0, 0, regencies[consensusCount], leaders[consensusCount], consId[consensusCount], cDecs[consensusCount].getConsMessages(), //... but there is still need to pass info about the consensus
                //        null, true); // there is no command that is the first of the batch, since it is a noop
                //msgCtx.setLastInBatch();
                //this.recoverer.noOp(msgCtx.getConsensusId(), msgCtx);
            }

            consensusCount++;
        }
        if (SVController.hasUpdates()) {

            this.scheduler.scheduleReplicaReconfiguration();

        }
    }

    /**
     * This method initializes the object
     *
     * @param cs Server side communication System
     * @param conf Total order messaging configuration
     */
    private void initTOMLayer() {
        if (tomStackCreated) { // if this object was already initialized, don't do it again
            return;
        }

        if (!SVController.isInCurrentView()) {
            throw new RuntimeException("I'm not an acceptor!");
        }

        // Assemble the total order messaging layer
        MessageFactory messageFactory = new MessageFactory(id);

        Acceptor acceptor = new Acceptor(cs, messageFactory, SVController);
        cs.setAcceptor(acceptor);

        Proposer proposer = new Proposer(cs, messageFactory, SVController);

        ExecutionManager executionManager = new ExecutionManager(SVController, acceptor, proposer, id);

        acceptor.setExecutionManager(executionManager);

        tomLayer = new ParallelTOMLayer(executionManager, this, recoverer, acceptor, cs, SVController, verifier);

        executionManager.setTOMLayer(tomLayer);

        SVController.setTomLayer(tomLayer);

        cs.setTOMLayer(tomLayer);
        cs.setRequestReceiver(tomLayer);

        acceptor.setTOMLayer(tomLayer);

        if (SVController.getStaticConf().isShutdownHookEnabled()) {
            Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(tomLayer));
        }
        tomLayer.start(); // start the layer execution
        tomStackCreated = true;

        replicaCtx = new ReplicaContext(cs, SVController);
    }

    private class ServiceReplicaWorker extends Thread {

        private Queue<TOMMessage> requests;
        private int thread_id;

        public ServiceReplicaWorker(Queue<TOMMessage> requests, int id) {
            this.thread_id = id;
            this.requests = requests;
        }

        int localC = 0;
        int localTotal = 0;

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

        private void execute(TOMMessage request) {

            MultiOperationRequest reqs = new MultiOperationRequest(request.getContent());
            MultiOperationCtx ctx = new MultiOperationCtx(reqs.id1.length, request);
            for (int i = 0; i < reqs.id1.length; i++) {
                this.execute(new MessageContextPair(request, request.groupId, i, reqs.id1[i], reqs.id2[i], reqs.opId, ctx));
            }
        }

        private void execute(MessageContextPair msg) {
            //msg.resp = ((SingleExecutable) executor).executeOrdered(msg.operation, null);
            msg.resp = ((SingleExecutable) executor).executeOrdered(serialize(msg.opId, msg.c1), null);
            msg.ctx.add(msg.index, msg.resp);
            if (msg.ctx.response.isComplete() && !msg.ctx.finished && (msg.ctx.interger.getAndIncrement() == 0)) {
                msg.ctx.finished = true;
                msg.ctx.request.reply = new TOMMessage(id, msg.ctx.request.getSession(),
                        msg.ctx.request.getSequence(), msg.ctx.response.serialize(), SVController.getCurrentViewId());

                replier.manageReply(msg.ctx.request, null);
            }
            statistics.computeStatistics(thread_id, 1);
        }

        public void run() {

            while (true) {

                TOMMessage msg = requests.poll();
                if (msg != null) {
                    try {
                        //EDUARDO: Não funciona com reconfiguração no conjunto de replicas, precisa colocar uma classe para isso em ClassToThreads com type = REC.
                        ClassToThreads ct = scheduler.getMapping().getClass(msg.getGroupId());

                        if (ct.type == ClassToThreads.CONC) {
                            //System.out.println("Thread " + thread_id + " vai executar uma operacao conflict none! " + msg.message.toString());
                            //sleep(5000);
                            //System.exit(0);
                            execute(msg);

                        } else if (ct.type == ClassToThreads.SYNC && ct.tIds.length == 1) {//SYNC mas só com 1 thread, não precisa usar barreira
                            execute(((TOMMessageWrapper) (msg)).msg);

                        } else if (ct.type == ClassToThreads.SYNC) {

                            if (thread_id == scheduler.getMapping().getExecutorThread(msg.getGroupId())) {

                                scheduler.getMapping().getBarrier(msg.getGroupId()).await();
                                // System.out.println("Thread " + thread_id + " );

                                //execute(msg);
                                execute(((TOMMessageWrapper) (msg)).msg);

                                scheduler.getMapping().getBarrier(msg.getGroupId()).await();

                            } else {
                                scheduler.getMapping().getBarrier(msg.getGroupId()).await();
                                //System.out.println(">>>Thread " + thread_id + " vai aguardar a execucao de uma operacao conflict: " + msg.message.getGroupId());
                                scheduler.getMapping().getBarrier(msg.getGroupId()).await();
                            }

                        } else if (msg.getGroupId() == ParallelMapping.CONFLICT_RECONFIGURATION) {
                            scheduler.getMapping().getReconfBarrier().await();
                            //System.out.println(">>>Thread " + thread_id + " vai aguardar uma reconfiguração!");
                            scheduler.getMapping().getReconfBarrier().await();

                        }
                    } catch (InterruptedException ex) {
                        Logger.getLogger(ParallelServiceReplica.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (BrokenBarrierException ex) {
                        Logger.getLogger(ParallelServiceReplica.class.getName()).log(Level.SEVERE, null, ex);
                    }

                }

            }
        }

        public void runOld() {
            /*MessageContextPair msg = null;

            ExecutionFIFOQueue<MessageContextPair> execQueue = new ExecutionFIFOQueue();

            while (true) {

                try {
                    //msg = requests.take();
                    this.requests.drainToQueue(execQueue);
                    localC++;
                    localTotal = localTotal + execQueue.getSize();
                    System.out.println("Thread " + thread_id + ": " + execQueue.getSize());

                    do {

                        msg = execQueue.getNext();
                        //System.out.println(">>>Thread " + thread_id + " PEGOU REQUEST");
                        //Thread.sleep(400);

                        //EDUARDO: Não funciona com reconfiguração no conjunto de replicas, precisa colocar uma classe para isso em ClassToThreads com type = REC.
                        ClassToThreads ct = scheduler.getMapping().getClass(msg.classId);

                        if (ct.type == ClassToThreads.CONC) {
                            //System.out.println("Thread " + thread_id + " vai executar uma operacao conflict none! " + msg.message.toString());
                            //sleep(5000);
                            //System.exit(0);

                            //msg.resp = ((SingleExecutable) executor).executeOrdered(msg.operation, null);
                            msg.resp = ((SingleExecutable) executor).executeOrdered(serialize(msg.opId, msg.operation), null);

                            //MultiOperationCtx ctx = ctxs.get(msg.request.toString());

                            msg.ctx.add(msg.index, msg.resp);

                            if (msg.ctx.response.isComplete() && !msg.ctx.finished && (msg.ctx.interger.getAndIncrement() == 0)) {
                                msg.ctx.finished = true;
                                msg.ctx.request.reply = new TOMMessage(id, msg.ctx.request.getSession(),
                                        msg.ctx.request.getSequence(), msg.ctx.response.serialize(), SVController.getCurrentViewId());
                                //bftsmart.tom.util.Logger.println("(ParallelServiceReplica.receiveMessages) sending reply to "+ msg.message.getSender());
                                replier.manageReply(msg.ctx.request, null);
                               
                            }
                            statistics.computeStatistics(thread_id, 1);

                        } else if (ct.type == ClassToThreads.SYNC && ct.tIds.length == 1) {//SYNC mas só com 1 thread, não precisa usar barreira
                            //msg.resp = ((SingleExecutable) executor).executeOrdered(msg.operation, null);
                            msg.resp = ((SingleExecutable) executor).executeOrdered(serialize(msg.opId, msg.operation), null);

                           // MultiOperationCtx ctx = ctxs.get(msg.request.toString());
                            msg.ctx.add(msg.index, msg.resp);
                            if (msg.ctx.response.isComplete() && !msg.ctx.finished && (msg.ctx.interger.getAndIncrement() == 0)) {
                                msg.ctx.finished = true;
                                msg.ctx.request.reply = new TOMMessage(id, msg.ctx.request.getSession(),
                                        msg.ctx.request.getSequence(), msg.ctx.response.serialize(), SVController.getCurrentViewId());
                                //bftsmart.tom.util.Logger.println("(ParallelServiceReplica.receiveMessages) sending reply to "
                                //      + msg.message.getSender());
                                replier.manageReply(msg.ctx.request, null);

                            }
                            statistics.computeStatistics(thread_id, 1);
                        } else if (ct.type == ClassToThreads.SYNC) {
                            if (thread_id == scheduler.getMapping().getExecutorThread(msg.classId)) {
                                scheduler.getMapping().getBarrier(msg.classId).await();
                                // System.out.println("Thread " + thread_id + " );
                                //msg.resp = ((SingleExecutable) executor).executeOrdered(msg.operation, null);
                                msg.resp = ((SingleExecutable) executor).executeOrdered(serialize(msg.opId, msg.operation), null);

                               // MultiOperationCtx ctx = ctxs.get(msg.request.toString());
                                msg.ctx.add(msg.index, msg.resp);
                                if (msg.ctx.response.isComplete() && !msg.ctx.finished && (msg.ctx.interger.getAndIncrement() == 0)) {
                                    msg.ctx.finished = true;
                                    msg.ctx.request.reply = new TOMMessage(id, msg.ctx.request.getSession(),
                                            msg.ctx.request.getSequence(), msg.ctx.response.serialize(), SVController.getCurrentViewId());
                                    //bftsmart.tom.util.Logger.println("(ParallelServiceReplica.receiveMessages) sending reply to "
                                    //      + msg.message.getSender());
                                    replier.manageReply(msg.ctx.request, null);

                                }
                                statistics.computeStatistics(thread_id, 1);
                                scheduler.getMapping().getBarrier(msg.classId).await();
                            } else {
                                scheduler.getMapping().getBarrier(msg.classId).await();
                                //System.out.println(">>>Thread " + thread_id + " vai aguardar a execucao de uma operacao conflict: " + msg.message.groupId);
                                scheduler.getMapping().getBarrier(msg.classId).await();
                            }

                        } else if (msg.classId == ParallelMapping.CONFLICT_RECONFIGURATION) {
                            scheduler.getMapping().getReconfBarrier().await();
                            //System.out.println(">>>Thread " + thread_id + " vai aguardar uma reconfiguração!");
                            scheduler.getMapping().getReconfBarrier().await();

                        }

                    } while (execQueue.goToNext());
                } catch (Exception ie) {
                    ie.printStackTrace();
                    continue;
                }

            }
             */
        }

    }

}
