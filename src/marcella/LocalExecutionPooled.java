/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marcella;

import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.util.MultiOperationRequest;
import demo.list.BFTList;
import demo.list.ListServerMP;
import demo.list.MultipartitionMapping;
import java.util.Arrays;
import java.util.Random;
import parallelism.MessageContextPair;
import parallelism.ParallelServiceReplica;
import parallelism.SequentialServiceReplica;
import parallelism.hybrid.early.EarlySchedulerMapping;

/**
 *
 * @author eduardo
 */
public class LocalExecutionPooled {

    protected int numberPartitions = 0;
    protected int maxIndex = 0;

    protected int numRequests = 0;
    
    protected int numLate = 0;

    protected int pg = 0;
    protected int pw = 0;

    private ListServerMP server;

    public static void main(String[] args) {
        int entries = Integer.parseInt(args[0]);//entries
        int lt = Integer.parseInt(args[1]);//workers
        int np = Integer.parseInt(args[2]); //partições
        int pG = Integer.parseInt(args[3]); //globais
        int pW = Integer.parseInt(args[4]); // writes
        int numReq = Integer.parseInt(args[5]); //num requests
        boolean n = Boolean.parseBoolean(args[6]); //é normal?
        //boolean hb = Boolean.parseBoolean(args[7]); //hybrid ?
        System.out.println("Normal "+n);
        
        PooledScheduler.normal = n;
        
        new LocalExecutionPooled(entries,lt,np,pG,pW,numReq);
     }
    
    public LocalExecutionPooled(int entries, int lateThreads, int numberPartitions, int pG, int pW, int numReqs) {
        this.pg = pG;
        this.pw = pW;
        this.numberPartitions = numberPartitions;
        this.maxIndex = entries;
        this.numRequests = numReqs;

        server = new ListServerMP(0, lateThreads, entries, 1, true, false, true);
        
        new Client(numRequests).start();
    }

    private class Client extends Thread {

        int opPerReq = 1;
        TOMMessage[] requests = null;
        public EarlySchedulerMapping em = new EarlySchedulerMapping();

        public Client(int numRequests) {
            createRequests(numRequests);
        }


        public void createRequests(int numRequests) {

            requests = new TOMMessage[numRequests];
            System.out.println("Executing experiment for " + requests.length + " ops");
            Random randOp = new Random();
            Random randPart = new Random();
            Random randGlobal = new Random();
            Random indexRand = new Random();
            Random confAll = new Random();
            Random partitonRand = new Random();

            //EarlySchedulerMapping em = new EarlySchedulerMapping();
            int[] allPartitions = new int[numberPartitions];
            for (int i = 0; i < numberPartitions; i++) {
                allPartitions[i] = i;
            }
            int[] conf = new int[2];

            for (int i = 0; i < requests.length; i++) {

                int g = randGlobal.nextInt(100);
                int r = randOp.nextInt(100);
                int p = 0;
                int op = 0;
                int multiP = 0;

                if (numberPartitions > 1 && g < pg) {//global
                    if (r < pw) {
                        //GW
                        p = -1;
                    } else {
                        //GR;
                        p = -2;
                    }

                    multiP = confAll.nextInt(100);
                    if (multiP >= 10) { //90% confitos entre dois shards (o resto é com todos os shards)
                        if (numberPartitions == 2) {
                            conf[0] = 0;
                            conf[1] = 1;
                        } else {
                            conf[0] = partitonRand.nextInt(numberPartitions);
                            do {
                                conf[1] = partitonRand.nextInt(numberPartitions);
                            } while (conf[0] == conf[1]);
                            Arrays.sort(conf);
                        }
                    }
                } else {//local
                    if (r < pw) {
                        op = BFTList.ADD;
                    } else {
                        op = BFTList.CONTAINS;
                    }
                }

                if (p == 0) {
                    switch (numberPartitions) {
                        case 1:
                            //1 partition
                            p = 1;
                            break;
                        case 2:
                            //2 partitions
                            r = randPart.nextInt(100);
                            if (r < 50) {
                                p = 1;
                            } else {
                                p = 2;
                            }
                            break;
                        case 4:
                            //4 partitions
                            r = randPart.nextInt(100);
                            if (r < 25) {
                                p = 1;
                            } else if (r < 50) {
                                p = 2;
                            } else if (r < 75) {
                                p = 3;
                            } else {
                                p = 4;
                            }
                            break;
                        case 6:
                            //6 partitions
                            r = randPart.nextInt(60);
                            if (r < 10) {
                                p = 1;
                            } else if (r < 20) {
                                p = 2;
                            } else if (r < 30) {
                                p = 3;
                            } else if (r < 40) {
                                p = 4;
                            } else if (r < 50) {
                                p = 5;
                            } else {
                                p = 6;
                            }
                            break;
                        default:
                            //8 partitions
                            r = randPart.nextInt(80);
                            if (r < 10) {
                                p = 1;
                            } else if (r < 20) {
                                p = 2;
                            } else if (r < 30) {
                                p = 3;
                            } else if (r < 40) {
                                p = 4;
                            } else if (r < 50) {
                                p = 5;
                            } else if (r < 60) {
                                p = 6;
                            } else if (r < 70) {
                                p = 7;
                            } else {
                                p = 8;
                            }
                            break;
                    }
                }

                if (p == -1) { //GW
                    //int index = maxIndex - 1;

                    Integer[] reqs = new Integer[opPerReq];
                    for (int x = 0; x < reqs.length; x++) {
                        //reqs[x] = index;
                        reqs[x] = indexRand.nextInt(maxIndex);
                    }

                    if (multiP >= 10) { //2 shards
                        int opId = 200 + (conf[0] + 1) * 10 + (conf[1] + 1);
                        requests[i] = addMultiPartition(reqs, opId, conf);

                    } else { //all shards
                        requests[i] = addMultiPartition(reqs, MultipartitionMapping.GW, allPartitions);
                    }

                } else if (p == -2) {//GR
                    //int index = maxIndex - 1;

                    Integer[] reqs = new Integer[opPerReq];
                    for (int x = 0; x < reqs.length; x++) {
                        //reqs[x] = index;
                        reqs[x] = indexRand.nextInt(maxIndex);
                    }

                    if (multiP >= 10) { //2 shards
                        int opId = 100 + (conf[0] + 1) * 10 + (conf[1] + 1);
                        requests[i] = containsMultiPartition(reqs, opId, conf);
                    } else { //all shards
                        requests[i] = containsMultiPartition(reqs, MultipartitionMapping.GR, allPartitions);
                    }

                } else if (op == BFTList.ADD) {
                    Integer[] reqs = new Integer[opPerReq];
                    for (int x = 0; x < reqs.length; x++) {
                        //  reqs[x] = index;
                        reqs[x] = indexRand.nextInt(maxIndex);
                    }

                    switch (p) {
                        case 1:
                            requests[i] = addP1(reqs);
                            break;
                        case 2:
                            requests[i] = addP2(reqs);
                            break;
                        case 3:
                            requests[i] = addP3(reqs);
                            break;
                        case 4:
                            requests[i] = addP4(reqs);
                            break;
                        case 5:
                            requests[i] = addP5(reqs);
                            break;
                        case 6:
                            requests[i] = addP6(reqs);
                            break;
                        case 7:
                            requests[i] = addP7(reqs);
                            break;
                        default:
                            requests[i] = addP8(reqs);
                            break;
                    }

                } else if (op == BFTList.CONTAINS) {
                    Integer[] reqs = new Integer[opPerReq];
                    for (int x = 0; x < reqs.length; x++) {
                        reqs[x] = indexRand.nextInt(maxIndex);
                    }
                    switch (p) {
                        case 1:
                            requests[i] = containsP1(reqs);
                            break;
                        case 2:
                            requests[i] = containsP2(reqs);
                            break;
                        case 3:
                            requests[i] = containsP3(reqs);
                            break;
                        case 4:
                            requests[i] = containsP4(reqs);
                            break;
                        case 5:
                            requests[i] = containsP5(reqs);
                            break;
                        case 6:
                            requests[i] = containsP6(reqs);
                            break;
                        case 7:
                            requests[i] = containsP7(reqs);
                            break;
                        default:
                            requests[i] = containsP8(reqs);
                            break;
                    }
                }

            }
            System.out.println("Requisições criadas!");
        }

        @Override
        public void run() {
            if (server.replica instanceof ParallelServiceReplica) {
                for (int i = 0; i < requests.length; i++) {
                    ((ParallelServiceReplica) server.replica).localExecution(requests[i]);
                }
            } else {
                for (int i = 0; i < requests.length; i++) {
                    ((SequentialServiceReplica) server.replica).localExecution(requests[i]);
                }
            }
            System.out.println("Fim do envio!");

        }

        public TOMMessage addP1(Object[] e) {
            return addFinal(e, 0, MultipartitionMapping.W1);
        }

        public TOMMessage addP2(Object[] e) {
            return addFinal(e, 1, MultipartitionMapping.W2);
        }

        public TOMMessage addP3(Object[] e) {
            return addFinal(e, 2, MultipartitionMapping.W3);
        }

        public TOMMessage addP4(Object[] e) {
            return addFinal(e, 3, MultipartitionMapping.W4);
        }

        public TOMMessage addP5(Object[] e) {
            return addFinal(e, 4, MultipartitionMapping.W5);
        }

        public TOMMessage addP6(Object[] e) {
            return addFinal(e, 5, MultipartitionMapping.W6);
        }

        public TOMMessage addP7(Object[] e) {
            return addFinal(e, 6, MultipartitionMapping.W7);
        }

        public TOMMessage addP8(Object[] e) {
            return addFinal(e, 7, MultipartitionMapping.W8);
        }

        public TOMMessage addFinal(Object[] e, int partition, int opId) {
            MultiOperationRequest mo = new MultiOperationRequest(e.length, (short) opId);
            for (int i = 0; i < e.length; i++) {
                short value = Short.valueOf(e[i].toString());
                mo.add(i, value);

            }
            //if (early) {
           //     return new TOMMessage(7001, 0, 0, 0, mo.serialize(), 0, TOMMessageType.ORDERED_REQUEST, em.getClassIdEarly("W", partition));
           // } else {
                return new TOMMessage(7001, 0, 0, 0, mo.serialize(), 0, TOMMessageType.ORDERED_REQUEST, em.getClassId(partition));
           // }

        }

        public TOMMessage addMultiPartition(Object[] e, int opId, int... partitions) {
            MultiOperationRequest mo = null;
            if (partitions.length > 0) {
                mo = new MultiOperationRequest(e.length, (short) opId);
            } else {
                mo = new MultiOperationRequest(e.length, (short) MultipartitionMapping.GW);
            }
            for (int i = 0; i < e.length; i++) {
                short value = Short.valueOf(e[i].toString());
                mo.add(i, value);

            }
            if (partitions.length > 0) {
               // if (early) {
                //    return new TOMMessage(7001, 0, 0, 0, mo.serialize(), 0, TOMMessageType.ORDERED_REQUEST, em.getClassIdEarly("W", partitions));
                //} else {
                    return new TOMMessage(7001, 0, 0, 0, mo.serialize(), 0, TOMMessageType.ORDERED_REQUEST, em.getClassId(partitions));
               // }
            } else {
                return new TOMMessage(7001, 0, 0, 0, mo.serialize(), 0, TOMMessageType.ORDERED_REQUEST, MultipartitionMapping.GW);
            }
        }

        public TOMMessage containsP1(Object[] e) {
            return containsFinal(e, 0, MultipartitionMapping.R1); // R1 = identificador
        }

        public TOMMessage containsP2(Object[] e) {
            return containsFinal(e, 1, MultipartitionMapping.R2);
        }

        public TOMMessage containsP3(Object[] e) {
            return containsFinal(e, 2, MultipartitionMapping.R3);
        }

        public TOMMessage containsP4(Object[] e) {
            return containsFinal(e, 3, MultipartitionMapping.R4);
        }

        public TOMMessage containsP5(Object[] e) {
            return containsFinal(e, 4, MultipartitionMapping.R5);
        }

        public TOMMessage containsP6(Object[] e) {
            return containsFinal(e, 5, MultipartitionMapping.R6);
        }

        public TOMMessage containsP7(Object[] e) {
            return containsFinal(e, 6, MultipartitionMapping.R7);
        }

        public TOMMessage containsP8(Object[] e) {

            return containsFinal(e, 7, MultipartitionMapping.R8);

        }

        public TOMMessage containsFinal(Object[] e, int partition, short opId) {
            MultiOperationRequest mo = new MultiOperationRequest(e.length, (short) opId);
            for (int i = 0; i < e.length; i++) {
                short value = Short.valueOf(e[i].toString());
                mo.add(i, value);
            }
           // if (early) {
             //   return new TOMMessage(7001, 0, 0, 0, mo.serialize(), 0, TOMMessageType.ORDERED_REQUEST, em.getClassIdEarly("R", partition));
           // } else {
                return new TOMMessage(7001, 0, 0, 0, mo.serialize(), 0, TOMMessageType.ORDERED_REQUEST, em.getClassId(partition));
          //  }
        }

        public TOMMessage containsMultiPartition(Object[] e, int opId, int... partitions) {
            // try {
            MultiOperationRequest mo = null;
            if (partitions.length > 0) {
                mo = new MultiOperationRequest(e.length, (short) opId);
            } else {
                mo = new MultiOperationRequest(e.length, (short) MultipartitionMapping.GR);
            }

            for (int i = 0; i < e.length; i++) {
                short value = Short.valueOf(e[i].toString());
                mo.add(i, value);
            }
            if (partitions.length > 0) {
               // if (early) {
                //    return new TOMMessage(7001, 0, 0, 0, mo.serialize(), 0, TOMMessageType.ORDERED_REQUEST, em.getClassIdEarly("R", partitions));
               // } else {
                    return new TOMMessage(7001, 0, 0, 0, mo.serialize(), 0, TOMMessageType.ORDERED_REQUEST, em.getClassId(partitions));
               // }
            } else {
                return new TOMMessage(7001, 0, 0, 0, mo.serialize(), 0, TOMMessageType.ORDERED_REQUEST, MultipartitionMapping.GR);
            }

        }

    }

}
