/**
 * Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and
 * the authors indicated in the @author tags
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package demo.map;

//import bftsmart.tom.parallelism.ParallelMapping;
import bftsmart.tom.ParallelServiceProxy;
import demo.list.*;
import bftsmart.tom.core.messages.TOMMessage;
import java.io.IOException;

import java.util.logging.Level;
import java.util.logging.Logger;
import bftsmart.util.ExtStorage;
import bftsmart.util.MultiOperationRequest;
import java.util.Arrays;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import parallelism.MessageContextPair;
import parallelism.hybrid.early.EarlySchedulerMapping;
import sun.security.provider.PolicyParser;

/**
 * Example client
 *
 */
public class CoinClient {

    public static int initId = 0;
    public static boolean stop = false;

    // protected static int pG = 5;
    // protected static int pW = 10;
    @SuppressWarnings("static-access")
    public static void main(String[] args) throws IOException {
        if (args.length < 8) {
            System.out.println("Usage: ... ListClient "
                    + "<num. threads> <process id> <number of requests> <interval> <maxIndex> <parallel?> <operations per request> <partitions>");
            System.exit(-1);
        }

        int numThreads = Integer.parseInt(args[0]);
        initId = Integer.parseInt(args[1]);

        /*if (initId == 7001 || initId == 1001 || initId == 2001) {

            op = BFTList.ADD;
        }*/
        int numberOfReqs = Integer.parseInt(args[2]);
        //int requestSize = Integer.parseInt(args[3]);
        int interval = Integer.parseInt(args[3]);
        int max = Integer.parseInt(args[4]);
        boolean parallel = Boolean.parseBoolean(args[5]);
        int numberOfOps = Integer.parseInt(args[6]);

        int p = Integer.parseInt(args[7]);

        Client[] c = new Client[numThreads];

        for (int i = 0; i < numThreads; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                Logger.getLogger(CoinClient.class.getName()).log(Level.SEVERE, null, ex);
            }

            System.out.println("Launching client " + (initId + i));
            c[i] = new CoinClient.Client(initId + i, numberOfReqs, numberOfOps, interval, max, p, parallel);
            //c[i].start();
        }

        try {
            Thread.sleep(300);
        } catch (InterruptedException ex) {
            Logger.getLogger(CoinClient.class.getName()).log(Level.SEVERE, null, ex);
        }

        for (int i = 0; i < numThreads; i++) {

            c[i].start();
        }

        (new Timer()).scheduleAtFixedRate(new TimerTask() {
            public void run() {
                //change();
            }
        }, 60000, 60000); //a cada 1 minuto

        (new Timer()).schedule(new TimerTask() {
            public void run() {
                stop();
            }
        }, 5 * 60000); //depois de 5 minutos

        for (int i = 0; i < numThreads; i++) {

            try {
                c[i].join();
            } catch (InterruptedException ex) {
                ex.printStackTrace(System.err);
            }
        }

        //System.exit(0);
    }

    public static void stop() {
        stop = true;
    }

    static class Client extends Thread {

        int id;
        int numberOfReqs;
        int interval;

        int countNumOp = 0;

        //public int op = BFTList.ADD;
        //boolean verbose;
        //boolean dos;
        //ServiceProxy proxy;
        //byte[] request;
        //BFTListMOMP<Integer> store;

        int maxIndex;
        //int percent;

        int opPerReq = 1;

        int partitions = 2;
        public EarlySchedulerMapping mapping;
        private ParallelServiceProxy proxy = null;

        public Client(int id, int numberOfRqs, int opPerReq, int interval, int maxIndex, int partitions, boolean parallel) {
            super("Client " + id);

            this.id = id;
            this.numberOfReqs = numberOfRqs;
            this.opPerReq = opPerReq;

            this.interval = interval;

            this.partitions = partitions;

            
            //this.dos = dos;

            this.mapping = new EarlySchedulerMapping();

            this.proxy =  new ParallelServiceProxy(id);
        }

        /*  private boolean insertValue(int index) {

            return store.add(index);

        }*/
        public void run() {
            //run_skewed();
            run_balanced();
        }

        private int getPartition(int c) {
            switch (partitions) {
                case 1:
                    return 1;
                case 2:
                    if( c < 399){
                        return 1;
                    }
                    return 2;
                case 4:
                    if( c < 199){
                        return 1;
                    }else if ( c < 399){
                        return 2;
                    }else if ( c < 599){
                        return 3;
                    }
                    return 4;
                case 6:
                    if( c < 134){
                        return 1;
                    }else if ( c < 267){
                        return 2;
                    }else if ( c < 400){
                        return 3;
                    }else if ( c < 533){
                        return 4;
                    }else if ( c < 666){
                        return 5;
                    }
                    return 6;
                default: //8 partitions
                    if( c < 99){
                        return 1;
                    }else if ( c < 199){
                        return 2;
                    }else if ( c < 299){
                        return 3;
                    }else if ( c < 399){
                        return 4;
                    }else if ( c < 499){
                        return 5;
                    }else if ( c < 599){
                        return 6;
                    }else if ( c < 699){
                        return 7;
                    }
                    return 8;
            }
        }

        public void run_balanced() {

            ExtStorage sR = new ExtStorage();
            ExtStorage sW = new ExtStorage();
            ExtStorage sGR = new ExtStorage();
            ExtStorage sGW = new ExtStorage();

            System.out.println("Executing experiment for " + numberOfReqs + " ops");

            Random randClient = new Random();

             Random randOp = new Random();



            int[] conf = new int[2];

            for (int i = 0; i < numberOfReqs && !stop; i++) {
                if (i == 1) {
                    try {
                        //Thread.currentThread().sleep(20000);
                        Thread.currentThread().sleep(20000);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(CoinClient.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }

         
                int r = randOp.nextInt(10);

                int c1 = randClient.nextInt(800);
                int c2 = -1;
                
                if(r < 7){
                    c2 = c1++;
                }else{
                    do {
                        c2 = randClient.nextInt(800);
                    } while (c1 == c2);
                }
                
                int p1 = getPartition(c1);
                int p2 = getPartition(c2);

                if (p1 == p2){ //vai para apenas uma partição
                    switch (p1) {
                        case 1:
                            //transferFinal(c1,c2, opPerReq,mapping.getClassId(0),MultipartitionMapping.W1);
                            transferFinal(c1,c2, opPerReq,mapping.getClassIdEarly("W",0),MultipartitionMapping.W1);
                            break;
                        case 2:
                            //transferFinal(c1,c2, opPerReq,mapping.getClassId(1),MultipartitionMapping.W2);
                            transferFinal(c1,c2, opPerReq,mapping.getClassIdEarly("W",1),MultipartitionMapping.W2);
                            break;
                        case 3:
                            //transferFinal(c1,c2, opPerReq,mapping.getClassId(2),MultipartitionMapping.W3);
                            transferFinal(c1,c2, opPerReq,mapping.getClassIdEarly("W",2),MultipartitionMapping.W3);
                            break;
                        case 4:
                            //transferFinal(c1,c2, opPerReq,mapping.getClassId(3),MultipartitionMapping.W4);
                            transferFinal(c1,c2, opPerReq,mapping.getClassIdEarly("W",3),MultipartitionMapping.W4);
                            break;
                        case 5:
                            //transferFinal(c1,c2, opPerReq,mapping.getClassId(4),MultipartitionMapping.W5);
                            transferFinal(c1,c2, opPerReq,mapping.getClassIdEarly("W",4),MultipartitionMapping.W5);
                            break;
                        case 6:
                            //transferFinal(c1,c2, opPerReq,mapping.getClassId(5),MultipartitionMapping.W6);
                            transferFinal(c1,c2, opPerReq,mapping.getClassIdEarly("W",5),MultipartitionMapping.W6);
                            break;
                        case 7:
                            //transferFinal(c1,c2, opPerReq,mapping.getClassId(6),MultipartitionMapping.W7);
                            transferFinal(c1,c2, opPerReq,mapping.getClassIdEarly("W",6),MultipartitionMapping.W7);
                            break;
                        case 8:
                            //transferFinal(c1,c2, opPerReq,mapping.getClassId(7),MultipartitionMapping.W8);
                            transferFinal(c1,c2, opPerReq,mapping.getClassIdEarly("W",7),MultipartitionMapping.W8);
                            break;
                        default:
                            break;
                    }
                }else{//vai para duas partições
                    
                    conf[0] = p1-1;
                    conf[1] = p2-1;
                    Arrays.sort(conf);
                    
                    int opId = 200 + (conf[0] + 1) * 10 + (conf[1] + 1);
                    
                    //transferFinal(c1,c2, opPerReq,mapping.getClassId(conf), opId);
                    transferFinal(c1,c2, opPerReq,mapping.getClassIdEarly("W", conf), opId);
                    
                }
                
                  


                if (interval > 0 && i % 50 == 100) {
                    try {
                        Thread.sleep(interval);
                    } catch (InterruptedException ex) {
                    }
                }

            }

            if (id == initId) {
                System.out.println(this.id + " //READ Average time for " + numberOfReqs + " executions (-10%) = " + sR.getAverage(true) / 1000 + " us ");
                System.out.println(this.id + " //READ Standard desviation for " + numberOfReqs + " executions (-10%) = " + sR.getDP(true) / 1000 + " us ");
                System.out.println(this.id + " // READ 90th percentile for " + numberOfReqs + " executions = " + sR.getPercentile(90) / 1000 + " us ");
                System.out.println(this.id + " // READ 95th percentile for " + numberOfReqs + " executions = " + sR.getPercentile(95) / 1000 + " us ");
                System.out.println(this.id + " // READ 99th percentile for " + numberOfReqs + " executions = " + sR.getPercentile(99) / 1000 + " us ");

                System.out.println(this.id + " //WRITE Average time for " + numberOfReqs + " executions (-10%) = " + sW.getAverage(true) / 1000 + " us ");
                System.out.println(this.id + " //WRITE Standard desviation for " + numberOfReqs + " executions (-10%) = " + sW.getDP(true) / 1000 + " us ");
                System.out.println(this.id + " // WRITE 90th percentile for " + numberOfReqs + " executions = " + sW.getPercentile(90) / 1000 + " us ");
                System.out.println(this.id + " // WRITE 95th percentile for " + numberOfReqs + " executions = " + sW.getPercentile(95) / 1000 + " us ");
                System.out.println(this.id + " // WRITE 99th percentile for " + numberOfReqs + " executions = " + sW.getPercentile(99) / 1000 + " us ");

                System.out.println(this.id + " //GLOBAL READ Average time for " + numberOfReqs + " executions (-10%) = " + sGR.getAverage(true) / 1000 + " us ");
                System.out.println(this.id + " //GLOBAL READ Standard desviation for " + numberOfReqs + " executions (-10%) = " + sGR.getDP(true) / 1000 + " us ");
                System.out.println(this.id + " //GLOBAL READ 90th percentile for " + numberOfReqs + " executions = " + sGR.getPercentile(90) / 1000 + " us ");
                System.out.println(this.id + " //GLOBAL READ 95th percentile for " + numberOfReqs + " executions = " + sGR.getPercentile(95) / 1000 + " us ");
                System.out.println(this.id + " //GLOBAL READ 99th percentile for " + numberOfReqs + " executions = " + sGR.getPercentile(99) / 1000 + " us ");

                System.out.println(this.id + " //GLOBAL WRITE Average time for " + numberOfReqs + " executions (-10%) = " + sGW.getAverage(true) / 1000 + " us ");
                System.out.println(this.id + " //GLOBAL WRITE Standard desviation for " + numberOfReqs + " executions (-10%) = " + sGW.getDP(true) / 1000 + " us ");
                System.out.println(this.id + " //GLOBAL WRITE 90th percentile for " + numberOfReqs + " executions = " + sGW.getPercentile(90) / 1000 + " us ");
                System.out.println(this.id + " //GLOBAL WRITE 95th percentile for " + numberOfReqs + " executions = " + sGW.getPercentile(95) / 1000 + " us ");
                System.out.println(this.id + " //GLOBAL WRITE 99th percentile for " + numberOfReqs + " executions = " + sGW.getPercentile(99) / 1000 + " us ");

            }
        }

        public void transferFinal(int c1, int c2, int size, int pId, int opId) {
         MultiOperationRequest mo = new MultiOperationRequest(size, (short) opId);
        for (int i = 0; i < size; i++) {


            /* out = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(out);
                dos.writeInt(opId);
                ObjectOutputStream out1 = new ObjectOutputStream(out);
                out1.writeObject(e[i]);
                out1.close();
                mo.add(i, out.toByteArray(), pId, opId);*/
            //short value = Short.valueOf(e[i].toString());
            mo.add(i, (short)c1, (short)c2);

        }
        
        //System.out.println(opId+" ReqID ADD: "+pId);
        
        byte[] rep = proxy.invokeParallel(mo.serialize(), pId);
         
         }
        
    }
}
