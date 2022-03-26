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
public class MapServerMP implements SingleExecutable {

    public ServiceReplica replica;

    int numberpartitions = 2;

    private TreeMap<Integer, String> map1 = new TreeMap<>();
    private TreeMap<Integer, String> map2 = new TreeMap<>();
    private TreeMap<Integer, String> map3 = new TreeMap<>();
    private TreeMap<Integer, String> map4 = new TreeMap<>();
    private TreeMap<Integer, String> map5 = new TreeMap<>();
    private TreeMap<Integer, String> map6 = new TreeMap<>();
    private TreeMap<Integer, String> map7 = new TreeMap<>();
    private TreeMap<Integer, String> map8 = new TreeMap<>();

    private byte[] signature;
    
    public MapServerMP(int id, int initThreads, int entries, int numberPartitions, boolean cbase, boolean hibrid, boolean pooled) {

        this.numberpartitions = numberPartitions;

        
        
        if (initThreads <= 0) {
            System.out.println("Replica in sequential execution model.");

            replica = new SequentialServiceReplica(id, this, null);
        } else if (cbase) {
            ConflictDefinition cd = new ConflictDefinition() {

                private boolean conflictR1(int opId) {
                    return opId == MultipartitionMapping.GW
                            || opId == MultipartitionMapping.W1
                            || (MultipartitionMapping.W12 <= opId && opId <= MultipartitionMapping.W18);
                }

                private boolean conflictW1(int opId) {
                    return conflictR1(opId)
                            || opId == MultipartitionMapping.GR
                            || opId == MultipartitionMapping.R1
                            || (MultipartitionMapping.R12 <= opId && opId <= MultipartitionMapping.R18);
                }

                private boolean conflictR2(int opId) {
                    return opId == MultipartitionMapping.GW
                            || opId == MultipartitionMapping.W2
                            || opId == MultipartitionMapping.W12
                            || (MultipartitionMapping.W23 <= opId && opId <= MultipartitionMapping.W28);
                }

                private boolean conflictW2(int opId) {
                    return conflictR2(opId)
                            || opId == MultipartitionMapping.GR
                            || opId == MultipartitionMapping.R2
                            || opId == MultipartitionMapping.R12
                            || (MultipartitionMapping.R23 <= opId && opId <= MultipartitionMapping.R28);
                }

                private boolean conflictR3(int opId) {
                    return opId == MultipartitionMapping.GW
                            || opId == MultipartitionMapping.W3
                            || opId == MultipartitionMapping.W13
                            || opId == MultipartitionMapping.W23
                            || (MultipartitionMapping.W34 <= opId && opId <= MultipartitionMapping.W38);
                }

                private boolean conflictW3(int opId) {
                    return conflictR3(opId)
                            || opId == MultipartitionMapping.GR
                            || opId == MultipartitionMapping.R3
                            || opId == MultipartitionMapping.R13
                            || opId == MultipartitionMapping.R23
                            || (MultipartitionMapping.R34 <= opId && opId <= MultipartitionMapping.R38);
                }

                private boolean conflictR4(int opId) {
                    return opId == MultipartitionMapping.GW
                            || opId == MultipartitionMapping.W4
                            || opId == MultipartitionMapping.W14
                            || opId == MultipartitionMapping.W24
                            || opId == MultipartitionMapping.W34
                            || (MultipartitionMapping.W45 <= opId && opId <= MultipartitionMapping.W48);
                }

                private boolean conflictW4(int opId) {
                    return conflictR4(opId)
                            || opId == MultipartitionMapping.GR
                            || opId == MultipartitionMapping.R4
                            || opId == MultipartitionMapping.R14
                            || opId == MultipartitionMapping.R24
                            || opId == MultipartitionMapping.R34
                            || (MultipartitionMapping.R45 <= opId && opId <= MultipartitionMapping.R48);
                }

                private boolean conflictR5(int opId) {
                    return opId == MultipartitionMapping.GW
                            || opId == MultipartitionMapping.W5
                            || opId == MultipartitionMapping.W15
                            || opId == MultipartitionMapping.W25
                            || opId == MultipartitionMapping.W35
                            || opId == MultipartitionMapping.W45
                            || (MultipartitionMapping.W56 <= opId && opId <= MultipartitionMapping.W58);
                }

                private boolean conflictW5(int opId) {
                    return conflictR5(opId)
                            || opId == MultipartitionMapping.GR
                            || opId == MultipartitionMapping.R5
                            || opId == MultipartitionMapping.R15
                            || opId == MultipartitionMapping.R25
                            || opId == MultipartitionMapping.R35
                            || opId == MultipartitionMapping.R45
                            || (MultipartitionMapping.R56 <= opId && opId <= MultipartitionMapping.R58);
                }

                private boolean conflictR6(int opId) {
                    return opId == MultipartitionMapping.GW
                            || opId == MultipartitionMapping.W6
                            || opId == MultipartitionMapping.W16
                            || opId == MultipartitionMapping.W26
                            || opId == MultipartitionMapping.W36
                            || opId == MultipartitionMapping.W46
                            || opId == MultipartitionMapping.W56
                            || opId == MultipartitionMapping.W67
                            || opId == MultipartitionMapping.W68;
                }

                private boolean conflictW6(int opId) {
                    return conflictR6(opId)
                            || opId == MultipartitionMapping.GR
                            || opId == MultipartitionMapping.R6
                            || opId == MultipartitionMapping.R16
                            || opId == MultipartitionMapping.R26
                            || opId == MultipartitionMapping.R36
                            || opId == MultipartitionMapping.R46
                            || opId == MultipartitionMapping.R56
                            || opId == MultipartitionMapping.R67
                            || opId == MultipartitionMapping.R68;
                }

                private boolean conflictR7(int opId) {
                    return opId == MultipartitionMapping.GW
                            || opId == MultipartitionMapping.W7
                            || opId == MultipartitionMapping.W17
                            || opId == MultipartitionMapping.W27
                            || opId == MultipartitionMapping.W37
                            || opId == MultipartitionMapping.W47
                            || opId == MultipartitionMapping.W57
                            || opId == MultipartitionMapping.W67
                            || opId == MultipartitionMapping.W78;
                }

                private boolean conflictW7(int opId) {
                    return conflictR7(opId)
                            || opId == MultipartitionMapping.GR
                            || opId == MultipartitionMapping.R7
                            || opId == MultipartitionMapping.R17
                            || opId == MultipartitionMapping.R27
                            || opId == MultipartitionMapping.R37
                            || opId == MultipartitionMapping.R47
                            || opId == MultipartitionMapping.R57
                            || opId == MultipartitionMapping.R67
                            || opId == MultipartitionMapping.R78;
                }

                private boolean conflictR8(int opId) {
                    return opId == MultipartitionMapping.GW
                            || opId == MultipartitionMapping.W8
                            || opId == MultipartitionMapping.W18
                            || opId == MultipartitionMapping.W28
                            || opId == MultipartitionMapping.W38
                            || opId == MultipartitionMapping.W48
                            || opId == MultipartitionMapping.W58
                            || opId == MultipartitionMapping.W68
                            || opId == MultipartitionMapping.W78;
                }

                private boolean conflictW8(int opId) {
                    return conflictR8(opId)
                            || opId == MultipartitionMapping.GR
                            || opId == MultipartitionMapping.R8
                            || opId == MultipartitionMapping.R18
                            || opId == MultipartitionMapping.R28
                            || opId == MultipartitionMapping.R38
                            || opId == MultipartitionMapping.R48
                            || opId == MultipartitionMapping.R58
                            || opId == MultipartitionMapping.R68
                            || opId == MultipartitionMapping.R78;
                }

                @Override
                public boolean isDependent(MessageContextPair r1, MessageContextPair r2) {

                    switch (r1.opId) {
                        case MultipartitionMapping.GR:
                            if (conflictR1(r2.opId)
                                    || conflictR2(r2.opId)
                                    || conflictR3(r2.opId)
                                    || conflictR4(r2.opId)
                                    || conflictR5(r2.opId)
                                    || conflictR6(r2.opId)
                                    || conflictR7(r2.opId)
                                    || conflictR8(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R1:
                            if (conflictR1(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R2:
                            if (conflictR2(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R3:
                            if (conflictR3(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R4:
                            if (conflictR4(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R5:
                            if (conflictR5(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R6:
                            if (conflictR6(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R7:
                            if (conflictR7(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R8:
                            if (conflictR8(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.GW:
                            return true;
                        case MultipartitionMapping.W1:
                            if (conflictW1(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W2:
                            if (conflictW2(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W3:
                            if (conflictW3(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W4:
                            if (conflictW4(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W5:
                            if (conflictW5(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W6:
                            if (conflictW6(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W7:
                            if (conflictW7(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W8:
                            if (conflictW8(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R12:
                            if (conflictR1(r2.opId) || conflictR2(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R13:
                            if (conflictR1(r2.opId) || conflictR3(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R14:
                            if (conflictR1(r2.opId) || conflictR4(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R15:
                            if (conflictR1(r2.opId) || conflictR5(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R16:
                            if (conflictR1(r2.opId) || conflictR6(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R17:
                            if (conflictR1(r2.opId) || conflictR7(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R18:
                            if (conflictR1(r2.opId) || conflictR8(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R23:
                            if (conflictR2(r2.opId) || conflictR3(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R24:
                            if (conflictR2(r2.opId) || conflictR4(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R25:
                            if (conflictR2(r2.opId) || conflictR5(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R26:
                            if (conflictR2(r2.opId) || conflictR6(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R27:
                            if (conflictR2(r2.opId) || conflictR7(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R28:
                            if (conflictR2(r2.opId) || conflictR8(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R34:
                            if (conflictR3(r2.opId) || conflictR4(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R35:
                            if (conflictR3(r2.opId) || conflictR5(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R36:
                            if (conflictR3(r2.opId) || conflictR6(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R37:
                            if (conflictR3(r2.opId) || conflictR7(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R38:
                            if (conflictR3(r2.opId) || conflictR8(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R45:
                            if (conflictR4(r2.opId) || conflictR5(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R46:
                            if (conflictR4(r2.opId) || conflictR6(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R47:
                            if (conflictR4(r2.opId) || conflictR7(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R48:
                            if (conflictR4(r2.opId) || conflictR8(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R56:
                            if (conflictR5(r2.opId) || conflictR6(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R57:
                            if (conflictR5(r2.opId) || conflictR7(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R58:
                            if (conflictR5(r2.opId) || conflictR8(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R67:
                            if (conflictR6(r2.opId) || conflictR7(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R68:
                            if (conflictR6(r2.opId) || conflictR8(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.R78:
                            if (conflictR7(r2.opId) || conflictR8(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W12:
                            if (conflictW1(r2.opId) || conflictW2(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W13:
                            if (conflictW1(r2.opId) || conflictW3(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W14:
                            if (conflictW1(r2.opId) || conflictW4(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W15:
                            if (conflictW1(r2.opId) || conflictW5(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W16:
                            if (conflictW1(r2.opId) || conflictW6(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W17:
                            if (conflictW1(r2.opId) || conflictW7(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W18:
                            if (conflictW1(r2.opId) || conflictW8(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W23:
                            if (conflictW2(r2.opId) || conflictW3(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W24:
                            if (conflictW2(r2.opId) || conflictW4(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W25:
                            if (conflictW2(r2.opId) || conflictW5(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W26:
                            if (conflictW2(r2.opId) || conflictW6(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W27:
                            if (conflictW2(r2.opId) || conflictW7(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W28:
                            if (conflictW2(r2.opId) || conflictW8(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W34:
                            if (conflictW3(r2.opId) || conflictW4(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W35:
                            if (conflictW3(r2.opId) || conflictW5(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W36:
                            if (conflictW3(r2.opId) || conflictW6(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W37:
                            if (conflictW3(r2.opId) || conflictW7(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W38:
                            if (conflictW3(r2.opId) || conflictW8(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W45:
                            if (conflictW4(r2.opId) || conflictW5(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W46:
                            if (conflictW4(r2.opId) || conflictW6(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W47:
                            if (conflictW4(r2.opId) || conflictW7(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W48:
                            if (conflictW4(r2.opId) || conflictW8(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W56:
                            if (conflictW5(r2.opId) || conflictW6(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W57:
                            if (conflictW5(r2.opId) || conflictW7(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W58:
                            if (conflictW5(r2.opId) || conflictW8(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W67:
                            if (conflictW6(r2.opId) || conflictW7(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W68:
                            if (conflictW6(r2.opId) || conflictW8(r2.opId)) {
                                return true;
                            }
                            break;
                        case MultipartitionMapping.W78:
                            if (conflictW7(r2.opId) || conflictW8(r2.opId)) {
                                return true;
                            }
                            break;

                        default:
                            break;
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
                replica = new LateServiceReplica(id, this, null, initThreads , cd, COSType.lockFreeGraph, numberPartitions);

            }
        } else {
            System.out.println("Replica in parallel execution model (early).");
            replica = new ParallelServiceReplica(id, this, null, (initThreads * numberPartitions), numberPartitions,
                    new EarlySchedulerMapping().generateEarly(numberPartitions, initThreads));

        }

        for (int i = 0; i < entries; i++) {
            this.map1.put(i, "Value_" + i);
            this.map2.put(i, "Value_" + i);
            this.map3.put(i, "Value_" + i);
            this.map4.put(i, "Value_" + i);
            this.map5.put(i, "Value_" + i);
            this.map6.put(i, "Value_" + i);
            this.map7.put(i, "Value_" + i);
            this.map8.put(i, "Value_" + i);
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
                boolean ret = add(value, cmd);
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
                out1.writeBoolean(contains(value, cmd));
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

    public boolean add(short v, int pId) {
        int value = Short.toUnsignedInt(v);

        boolean ret = false;
        switch (pId) {
            case MultipartitionMapping.W1:
                if (!map1.containsKey(value)) {
                    map1.put(value, "Value_" + value);
                    ret = true;
                }
                return ret;
            case MultipartitionMapping.W2:
                if (!map2.containsKey(value)) {
                    map2.put(value, "Value_" + value);
                    ret = true;
                }
                return ret;
            case MultipartitionMapping.W3:
                if (!map3.containsKey(value)) {
                    map3.put(value, "Value_" + value);
                    ret = true;
                }
                return ret;
            case MultipartitionMapping.W4:
                if (!map4.containsKey(value)) {
                    map4.put(value, "Value_" + value);
                    ret = true;
                }
                return ret;
            case MultipartitionMapping.W5:
                if (!map5.containsKey(value)) {
                    map5.put(value, "Value_" + value);
                    ret = true;
                }
                return ret;
            case MultipartitionMapping.W6:
                if (!map6.containsKey(value)) {
                    map6.put(value, "Value_" + value);
                    ret = true;
                }
                return ret;
            case MultipartitionMapping.W7:
                if (!map7.containsKey(value)) {
                    map7.put(value, "Value_" + value);
                    ret = true;
                }
                return ret;
            case MultipartitionMapping.W8:
                if (!map8.containsKey(value)) {
                    map8.put(value, "Value_" + value);
                    ret = true;
                }
                return ret;
            case MultipartitionMapping.GW:
                if (!map1.containsKey(value)) {
                    map1.put(value, "Value_" + value);
                    ret = true;
                }
                if (!map2.containsKey(value)) {
                    map2.put(value, "Value_" + value);
                    ret = true;
                }

                if (numberpartitions >= 4) {
                    if (!map3.containsKey(value)) {
                        map3.put(value, "Value_" + value);
                        ret = true;
                    }
                    if (!map4.containsKey(value)) {
                        map4.put(value, "Value_" + value);
                        ret = true;
                    }

                    if (numberpartitions >= 6) {

                        if (!map5.containsKey(value)) {
                            map5.put(value, "Value_" + value);
                            ret = true;
                        }
                        if (!map6.containsKey(value)) {
                            map6.put(value, "Value_" + value);
                            ret = true;
                        }

                        if (numberpartitions >= 8) {
                            if (!map7.containsKey(value)) {
                                map7.put(value, "Value_" + value);
                                ret = true;
                            }
                            if (!map8.containsKey(value)) {
                                map8.put(value, "Value_" + value);
                                ret = true;
                            }
                        }
                    }
                }

                return ret;
            default: //conflito de add em dois shards... pego dois qualquer, pois não adiciona por causa do contains

                if (!map1.containsKey(value)) {
                    map1.put(value, "Value_" + value);
                    ret = true;
                }
                if (!map2.containsKey(value)) {
                    map2.put(value, "Value_" + value);
                    ret = true;
                }

                break;
        }
        return ret;
    }

    public boolean contains(short v, int pId) {
        int value = Short.toUnsignedInt(v);
        //System.out.println(value);
        boolean ret = false;
        switch (pId) {
            case MultipartitionMapping.R1:
                map1.containsKey(value);
                break;
            case MultipartitionMapping.R2:
                map2.containsKey(value);
                break;
            case MultipartitionMapping.R3:
                map3.containsKey(value);
                break;
            case MultipartitionMapping.R4:
                map4.containsKey(value);
                break;
            case MultipartitionMapping.R5:
                map5.containsKey(value);
                break;
            case MultipartitionMapping.R6:
                map6.containsKey(value);
                break;
            case MultipartitionMapping.R7:
                map7.containsKey(value);
                break;
            case MultipartitionMapping.R8:
                map8.containsKey(value);
                break;
            case MultipartitionMapping.GR:

                if (this.numberpartitions == 2) {
                    map1.containsKey(value);
                    map2.containsKey(value);
                } else if (this.numberpartitions == 4) {
                    map1.containsKey(value);
                    map2.containsKey(value);
                    map3.containsKey(value);
                    map4.containsKey(value);

                } else if (this.numberpartitions == 6) {
                    map1.containsKey(value);
                    map2.containsKey(value);
                    map3.containsKey(value);
                    map4.containsKey(value);
                    map5.containsKey(value);
                    map6.containsKey(value);
                } else { // 8 partitions 
                    map1.containsKey(value);
                    map2.containsKey(value);
                    map3.containsKey(value);
                    map4.containsKey(value);
                    map5.containsKey(value);
                    map6.containsKey(value);
                    map7.containsKey(value);
                    map8.containsKey(value);
                }
                break;
            default: //conflito de contains em dois shards... pego dois qualquer (pois o add nao adiciona, dai nao tem problema)
                map1.containsKey(value);
                map2.containsKey(value);
                break;
        }
        return ret;
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

        new MapServerMP(processId, initialNT, entries, part, cbase, h, false);
    }

}