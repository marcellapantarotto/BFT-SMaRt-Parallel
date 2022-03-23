/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.hybrid.late;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;

import parallelism.MessageContextPair;
import parallelism.late.ConflictDefinition;
import parallelism.late.graph.Vertex;

/**
 *
 * @author eduardo
 */
public class ExtendedLockFreeGraph {

    private HybridLockFreeNode head;
    private HybridLockFreeNode tail;

    // private Semaphore space = null;                // counting semaphore for size of graph
    public Semaphore ready = new Semaphore(0);  // tells if there is ready to execute
    public Semaphore space = null;
    protected ConflictDefinition cd;

    //public HibridCOS cos;

    public int myPartition;

    public List<HybridLockFreeNode> checkDep;

    public ExtendedLockFreeGraph(ConflictDefinition cd, int myPartition, int subGraphSize) {
        head = new HybridLockFreeNode(null, Vertex.HEAD, this,0,0);
        tail = new HybridLockFreeNode(null, Vertex.TAIL, this,0,0);
        head.setNext(tail);
        this.cd = cd;
        this.myPartition = myPartition;
        this.checkDep = new LinkedList<>();
        this.space = new Semaphore(subGraphSize);
    }

    private boolean isDependent(MessageContextPair thisRequest, MessageContextPair otherRequest) {
        return this.cd.isDependent(thisRequest, otherRequest);
    }


    public HybridLockFreeNode get() throws InterruptedException {
       // if (this.ready.tryAcquire()) {
            this.ready.acquire();
            HybridLockFreeNode aux = (HybridLockFreeNode) head;
            boolean found = false;
            //int c = 0;
            while (!found) {
                if (aux.getVertex() == Vertex.TAIL) {
                    //System.out.println(Thread.currentThread().getId()+" TAIL,TAIL,TAIL,TAIL,TAIL,TAIL,TAIL,TAIL,TAIL,TAIL,TAIL,TAIL "+c);
                    //c++;
                    aux = (HybridLockFreeNode) head;
                }

                aux = (HybridLockFreeNode) aux.getNext();
                /*if(aux.graph == this){
                    System.out.println("************************************************** EH THIS");
                }else{
                    System.out.println("--- NAO EH THIS ---");
                }*/

                if (aux.readyAtomic.get()) { //was marked as ready
                    found = aux.reservedAtomic.compareAndSet(false, true);  // atomically set to reserve for exec
                }

                /*if(found){
                    System.out.println("************************************************** ENCONTROU");
                    
                }else{
                    System.out.println("----------------------------------------------------------------- NAO ENCONTROU ---");
                }*/
            }
            return aux;
        //}
        //return null;

    }

     public void remove(HybridLockFreeNode o) throws InterruptedException {
        //DUVIDA: acredito que não precisa ser atomico!
        //o.removedAtomic.compareAndSet(false, true);
        o.removed = true;
        o.testDepMeReady(); //post em ready dos grafos com novos nós prontos para execução
        this.space.release();
    }
     
      public void insert(HybridLockFreeNode newvNode, boolean dependencyOnly, boolean conflic) {
          if(dependencyOnly){
              insertDependencies(newvNode);
          }else{
              try {
                  this.space.acquire();
                  insertNodeAndDependencies(newvNode);                 
                  
              } catch (InterruptedException ex) {
                     ex.printStackTrace();
              }
          }
          if(conflic){
              if(newvNode.atomicCounter.decrementAndGet() == 0){
                  newvNode.inserted = true;
                  newvNode.testReady();
              }
          }else{
              newvNode.inserted = true;
              newvNode.testReady();
          }
          
      }
    
    private void insertDependencies(HybridLockFreeNode newvNode) {
        HybridLockFreeNode aux = (HybridLockFreeNode) head;
        HybridLockFreeNode aux2 = (HybridLockFreeNode) aux.getNext();
        while (aux2.getVertex() != Vertex.TAIL) {
            //HELPED REMOVE
            while (aux2.isRemoved()) {            // aux2 was removed, have to help
                //Se quiser da pra remover aux2 das listas depOn de quem depende dele -- poderia melhorar o testReady
                aux.setNext(aux2.getNext());        // bypass it on the linked list
                aux2 = (HybridLockFreeNode) aux.getNext();               // proceed with aux2 to next node
            }
            // this helps removing several consecutive marked to remove
            // in the limit case, aux2 is tail
            if ((aux.getVertex() != Vertex.HEAD)
                    && isDependent(newvNode.getAsRequest(), aux.getAsRequest())) {//if node conflicts
                //newvNode.dependsMore();                    // new node depends on one more
                newvNode.insertDepOn(aux, myPartition);
                aux.insert(newvNode,myPartition);  		               // add edge from older to newer

            }
            if (aux2.getVertex() != Vertex.TAIL) {
                aux2 = (HybridLockFreeNode) aux2.getNext();
                aux = (HybridLockFreeNode) aux.getNext();
            }
        }
        if ((aux.getVertex() != Vertex.HEAD)
                && isDependent(newvNode.getAsRequest(), aux.getAsRequest())) { //if node conflicts
            //newvNode.dependsMore(); // new node depends on one more
            newvNode.insertDepOn(aux, myPartition);
            aux.insert(newvNode,myPartition);
        }                                                  // added all needed edges TO new node

        Iterator<HybridLockFreeNode> it = this.checkDep.iterator();
        while (it.hasNext()) {
            HybridLockFreeNode next = it.next();
            if (next.isRemoved()) {
                it.remove();
            }else{
                if (newvNode.getAsRequest().classId != next.getAsRequest().classId &&
                        isDependent(newvNode.getAsRequest(), next.getAsRequest())) {//if node conflicts
                    newvNode.insertDepOn(next, myPartition);
                    next.insert(newvNode,myPartition);  		               // add edge from older to newer
                    //System.out.println(newvNode.getAsRequest()+" depende1 de "+next.getAsRequest());
                }
            }
        }
       
        this.checkDep.add(newvNode);

    }

    private void insertNodeAndDependencies(HybridLockFreeNode newvNode) throws InterruptedException {
        HybridLockFreeNode aux = (HybridLockFreeNode) head;
        HybridLockFreeNode aux2 = (HybridLockFreeNode) aux.getNext();

        while (aux2.getVertex() != Vertex.TAIL) {
            //HELPED REMOVE
            while (aux2.isRemoved()) {            // aux2 was removed, have to help
                //Se quiser da pra remover aux2 das listas depOn de quem depende dele -- poderia melhorar o testReady
                aux.setNext(aux2.getNext());        // bypass it on the linked list
                aux2 = (HybridLockFreeNode) aux.getNext();               // proceed with aux2 to next node
            }                                       // this helps removing several consecutive marked to remove
            // in the limit case, aux2 is tail
            if ((aux.getVertex() != Vertex.HEAD)
                    && isDependent(newvNode.getAsRequest(), aux.getAsRequest())) {//if node conflicts
                //newvNode.dependsMore();                    // new node depends on one more
                newvNode.insertDepOn(aux, myPartition);
                aux.insert(newvNode,myPartition);  		               // add edge from older to newer

            }
            if (aux2.getVertex() != Vertex.TAIL) {
                aux2 = (HybridLockFreeNode) aux2.getNext();
                aux = (HybridLockFreeNode) aux.getNext();
            }
        }
        if ((aux.getVertex() != Vertex.HEAD)
                && isDependent(newvNode.getAsRequest(), aux.getAsRequest())) { //if node conflicts
            //newvNode.dependsMore(); // new node depends on one more
            newvNode.insertDepOn(aux, myPartition);
            aux.insert(newvNode,myPartition);

        }                                                  // added all needed edges TO new node
        newvNode.setNext(tail);                            // at the end of the list
        aux.setNext(newvNode);                             // insert new node

        Iterator<HybridLockFreeNode> it = this.checkDep.iterator();
        while (it.hasNext()) {
            HybridLockFreeNode next = it.next();
            if (next.isRemoved()) {
                it.remove();
            }else{
                if (isDependent(newvNode.getAsRequest(), next.getAsRequest())) {//if node conflicts
                    newvNode.insertDepOn(next, myPartition);
                    next.insert(newvNode,myPartition);  		               // add edge from older to newer
                    //System.out.println(newvNode.getAsRequest()+" depende1 de "+next.getAsRequest());
                }
            }
        }
     
        //newvNode.inserted = true;
        //int rdy = newvNode.testReady();
        //return rdy;
    }

}
