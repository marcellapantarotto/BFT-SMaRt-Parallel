/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package demo.map;

import bftsmart.reconfiguration.util.TOMConfiguration;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.tom.util.TOMUtil;
//import com.codahale.metrics.ConsoleReporter;
//import com.codahale.metrics.CsvReporter;
//import com.codahale.metrics.MetricRegistry;
import demo.list.MultipartitionMapping;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
//import marcella.PooledServiceReplica;
import parallelism.MessageContextPair;
import parallelism.ParallelServiceReplica;
import parallelism.SequentialServiceReplica;
import parallelism.hybrid.HybridServiceReplica;
import parallelism.hybrid.early.EarlySchedulerMapping;
import parallelism.late.COSType;
import parallelism.late.ConflictDefinition;
import parallelism.late.LateServiceReplica;

/**
 *
 * @author eduardo
 */
public class CoinServerMP implements SingleExecutable {

    public ServiceReplica replica;

    int numberpartitions = 2;

    private byte[] signature;
    
    public CoinServerMP(int id, int initThreads, int entries, int numberPartitions, boolean cbase, boolean hibrid, boolean pooled) {

        this.numberpartitions = numberPartitions;

        
        
        if (initThreads <= 0) {
            System.out.println("Replica in sequential execution model.");

            replica = new SequentialServiceReplica(id, this, null);
        } else if (cbase) {
            ConflictDefinition cd = new ConflictDefinition() {


                @Override
                public boolean isDependent(MessageContextPair r1, MessageContextPair r2) {
                    if( r1.c1 == r2.c1 || r1.c1 == r2.c2 || r1.c2 == r2.c1 || r1.c2 == r2.c2){
                        return true;
                    }
                    return false;
                }
            };

            if (hibrid) {
                System.out.println("Replica in parallel execution model (HYBRID).");
                initThreads = initThreads * numberPartitions;
                replica = new HybridServiceReplica(id, this, null, numberPartitions, cd, initThreads);
            } else if (pooled) {
                System.out.println("Replica in parallel execution model (POOLED).");
                /*MetricRegistry metrics = new MetricRegistry();

                replica = new PooledServiceReplica(id, initThreads, this, null, cd, metrics);

                File metricsPath = createMetricsDirectory();
                CsvReporter csvReporter = CsvReporter.forRegistry(metrics).convertRatesTo(TimeUnit.SECONDS).build(metricsPath);
                csvReporter.start(1, TimeUnit.SECONDS);

                ConsoleReporter consoleReporter = ConsoleReporter
                        .forRegistry(metrics)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .build();
                consoleReporter.start(10, TimeUnit.SECONDS);*/

            } else {
                System.out.println("Replica in parallel execution model (CBASE).");
                initThreads = initThreads * numberPartitions;
                replica = new LateServiceReplica(id, this, null, initThreads , cd, COSType.lockFreeGraph, numberPartitions);

            }
        } else {
            System.out.println("Replica in parallel execution model (early).");
            replica = new ParallelServiceReplica(id, this, null, (initThreads * numberPartitions), numberPartitions,
                    new EarlySchedulerMapping().generateEarly(numberPartitions, initThreads));

        }


        signature = TOMUtil.signMessage(replica.getReplicaContext().getStaticConfiguration().getRSAPrivateKey(), new String("CASA").getBytes());
        
        System.out.println("Server initialization complete!");
    }

   /* private static File createMetricsDirectory() {
        File dir = new File("./metrics");
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                System.out.println("Can not create ./metrics directory.");
                System.exit(1);
            }
        } else if (!dir.isDirectory()) {
            System.out.println("./metrics must be a directory");
            System.exit(1);
        }
        return dir;
    }*/


    
    public byte[] executeOrdered(byte[] command, MessageContext msgCtx) {
        return execute(command, msgCtx);
    }

    public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
        return execute(command, msgCtx);
    }

    public byte[] execute(byte[] command, MessageContext msgCtx) {

        TOMUtil.verifySignature(replica.getReplicaContext().getStaticConfiguration().getRSAPublicKey(), new String("CASA").getBytes(), signature);
        
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(command);
            ByteArrayOutputStream out = null;
            byte[] reply = null;

            DataInputStream dataIn = new DataInputStream(in);

            short cmd = dataIn.readShort();
            if (cmd == MultipartitionMapping.GW
                    || //global write
                    (cmd >= 21 && cmd <= 28)
                    || //single-shard write
                    cmd > 200) { //2 shards write
                //Integer value = (Integer) new ObjectInputStream(in).readObject();
                short value = dataIn.readShort();
                boolean ret = true; //add(value, cmd);
                out = new ByteArrayOutputStream();
                ObjectOutputStream out1 = new ObjectOutputStream(out);
                out1.writeBoolean(ret);
                out.flush();
                out1.flush();
                reply = out.toByteArray();

            } else { //contains

                //Integer value = (Integer) new ObjectInputStream(in).readObject();
                short value = dataIn.readShort();
                out = new ByteArrayOutputStream();
                ObjectOutputStream out1 = new ObjectOutputStream(out);
                //out1.writeBoolean(contains(value, cmd));
                out1.writeBoolean(true);
                out.flush();
                out1.flush();
                reply = out.toByteArray();

            }
            return reply;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }

    }

    

    public static void main(String[] args) {
        if (args.length < 6) {
            System.out.println("Usage: ... ListServer <processId> <number partitions> <num workers> <initial entries> <CBASE?> <hibrid scheduling?>");
            System.exit(-1);
        }

        int processId = Integer.parseInt(args[0]);
        int part = Integer.parseInt(args[1]);
        int initialNT = Integer.parseInt(args[2]);
        int entries = Integer.parseInt(args[3]);
        boolean cbase = Boolean.parseBoolean(args[4]);
        boolean h = Boolean.parseBoolean(args[5]);

        new CoinServerMP(processId, initialNT, entries, part, cbase, h, false);
    }

}