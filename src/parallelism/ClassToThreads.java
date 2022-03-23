/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism;


import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CyclicBarrier;

/**
 *
 * @author eduardo
 */
public class ClassToThreads {
    
    public static int CONC = 0; // concurrent
    public static int SYNC = 1; //synchronized
    public int type;
    public int[] tIds;
    public int classId;
    
    public int executorIndex = 0;
    
    public Queue[] queues;
    public CyclicBarrier barrier;
    
    public  ClassToThreads(int classId, int type, int[] ids) {
        this.classId = classId;
        this.type = type;
        this.tIds = ids;
        
        System.out.println("ClasssID: "+classId);
    }
        
    public void setQueues(Queue[] q){
        if(q.length != tIds.length){
            System.err.println("INCORRECT MAPPING");
        }
        this.queues = q;
        if(this.type == SYNC){
            this.barrier = new CyclicBarrier(tIds.length);
        }
    }
    
    public String toString(){
        String t = "CONC";
        if(type == SYNC){
            t = "SYNC";
        }
         StringBuilder sb = new StringBuilder();
            for (int j = 0; j < tIds.length; j++) {
                //System.out.print(iv[j]);
                sb.append(tIds[j]+",");
            }
        
        
        return "CtoT [type:"+t+",classID:"+classId+", threads:"+sb.toString()+"]";
    }
    
}
