/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marcella;

import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.server.Executable;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.SingleExecutable;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import parallelism.MessageContextPair;
import parallelism.MultiOperationCtx;
import parallelism.ParallelServiceReplica;
import parallelism.late.ConflictDefinition;

import static com.codahale.metrics.MetricRegistry.name;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

public final class PooledServiceReplica extends ParallelServiceReplica {

    private static final class Stats {

        //final Meter commands;
        final Meter requests;

        Stats(MetricRegistry metrics) {
            //commands = metrics.meter(name(PooledScheduler.class, "commands"));
            requests = metrics.meter(name(PooledScheduler.class, "requests"));
        }

    }

    private final Stats stats;

    public PooledServiceReplica(int processID,
            int nThreads,
            Executable executor,
            Recoverable recover,
            ConflictDefinition cf,
            MetricRegistry metrics) {
        super(processID, executor, recover, new PooledScheduler(nThreads, cf, metrics));
        stats = new Stats(metrics);
    }

    @Override
    public void localExecution(TOMMessage req) {
        this.scheduler.schedule(req);
    }

    @Override
    protected void initWorkers(int nThreads, int processID) {
        PooledScheduler scheduler = (PooledScheduler) this.scheduler;
        scheduler.setExecutor(this::execute);
    }

    private void execute(MessageContextPair msg) {
        //System.out.println("Chamou execute**********");
        //TODO

        do {
            msg.resp = ((SingleExecutable) executor).executeOrdered(serialize(msg.opId, msg.c1), null);
            //stats.commands.mark();
            stats.requests.mark();
            msg = msg.next;

        } while (msg != null);
        /*
        msg.ctx.add(msg.index, msg.resp);
        if (msg.ctx.response.isComplete() && !msg.ctx.finished && (msg.ctx.interger.getAndIncrement() == 0)) {
            msg.ctx.finished = true;
            msg.ctx.request.reply = new TOMMessage(id, msg.ctx.request.getSession(),
                    msg.ctx.request.getSequence(), msg.ctx.response.serialize(), SVController.getCurrentViewId());

            //TODO: descomentar quando for executar de forma replicada, com clientes
            //replier.manageReply(msg.ctx.request, null);
            stats.requests.mark();
        }*/
    }

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
}
