/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.late;

import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.util.MultiOperationRequest;
import java.util.ArrayList;
import java.util.List;
import parallelism.late.graph.COS;
import parallelism.MessageContextPair;
import parallelism.MultiOperationCtx;
import parallelism.ParallelMapping;
import parallelism.late.graph.CoarseGrainedLock;
import parallelism.late.graph.FineGrainedLock;
import parallelism.late.graph.LockFreeGraph;
import parallelism.scheduler.Scheduler;

/**
 *
 * @author eduardo
 */
public class LateScheduler implements Scheduler {

    private COS cos;
    private int numWorkers;

    //private ConflictDefinition conflictDef;
    private static final int MAX_SIZE = 150;

    private final List<TOMMessage> requestList = new ArrayList<TOMMessage>();
    private final List<MultiOperationRequest> reqsList = new ArrayList<>();
    private final List<MultiOperationCtx> ctxList = new ArrayList<>();
    private final List<Integer> indexList = new ArrayList<>();
    private final List<Integer> groupIdList = new ArrayList<>();
    private final List<Short> operationList = new ArrayList<>();
    private final List<Short> opIdList = new ArrayList<>();

    public LateScheduler(int numWorkers, COSType cosType) {
        this(null, numWorkers, cosType);
    }

    public LateScheduler(ConflictDefinition cd, int numWorkers, COSType cosType) {
        //cos = new COS(150,graphType,this);
        int limit = 150;

        if (cd == null) {
            cd = new DefaultConflictDefinition();
        }

        if (cosType == null || cosType == COSType.coarseLockGraph) {
            this.cos = new CoarseGrainedLock(limit, cd);
        } else if (cosType == COSType.fineLockGraph) {
            this.cos = new FineGrainedLock(limit, cd);
        } else if (cosType == COSType.lockFreeGraph) {
            this.cos = new LockFreeGraph(limit, cd);
        } else {
            this.cos = new CoarseGrainedLock(limit, cd);
        }
        this.numWorkers = numWorkers;
    }

    /*public boolean isDependent(MessageContextPair thisRequest, MessageContextPair otherRequest){
        if(thisRequest.classId == ParallelMapping.CONFLICT_RECONFIGURATION || 
                otherRequest.classId == ParallelMapping.CONFLICT_RECONFIGURATION){
            return true;
        }
        return this.conflictDef.isDependent(thisRequest, otherRequest);
    }*/
    @Override
    public int getNumWorkers() {
        return this.numWorkers;
    }

    @Override
    public void schedule(TOMMessage request) {
        requestList.add(request);
        reqsList.add(new MultiOperationRequest(request.getContent()));

        if (requestList.size() == MAX_SIZE) {
            for (int i = 0; i < requestList.size(); i++) {
                ctxList.add(new MultiOperationCtx(reqsList.get(i).operations.length, request));
                groupIdList.add(requestList.get(i).groupId);
                opIdList.add(reqsList.get(i).opId);

                for (int j = 0; j < reqsList.get(i).operations.length; j++) {
                    indexList.add(j);
                    operationList.add(reqsList.get(i).operations[j]);
                }

                this.schedule(new MessageContextPair(requestList, groupIdList, indexList, operationList, opIdList, ctxList));
            }

            requestList.clear();
            groupIdList.clear();
            indexList.clear();
            operationList.clear();
            opIdList.clear();
            ctxList.clear();
            reqsList.clear();
        }
    }

    @Override
    public void schedule(MessageContextPair request) {
        try {
            cos.insert(request);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }

    }

    public Object get() {

        try {
            return cos.get();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public void remove(Object requestRequest) {
        try {
            cos.remove(requestRequest);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void scheduleReplicaReconfiguration() {
        List<TOMMessage> requestListAux = new ArrayList<>(null);
        List<Integer> groupIdListAux = new ArrayList<>(ParallelMapping.CONFLICT_RECONFIGURATION);
        List<Integer> indexListAux = new ArrayList<>(-1);
        List<Short> operationListAux = new ArrayList<>((short) 0);
        List<Short> opIdListAux = new ArrayList<>((short) ParallelMapping.CONFLICT_RECONFIGURATION);
        List<MultiOperationCtx> ctxListAux = new ArrayList<>(null);

        MessageContextPair m
                = new MessageContextPair(requestListAux, groupIdListAux, indexListAux, operationListAux, opIdListAux, ctxListAux);
        schedule(m);
    }

    @Override
    public ParallelMapping getMapping() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
