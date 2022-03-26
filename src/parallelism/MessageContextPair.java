/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism;

import bftsmart.tom.core.messages.TOMMessage;
import parallelism.hybrid.late.HybridLockFreeNode;

/**
 *
 * @author eduardo
 */
public class MessageContextPair {

    public TOMMessage request;
    public int classId;

    //public short operation;
    public short c1;
    public short c2;

    public int index;
    public byte[] resp;

    public short opId;

    public MultiOperationCtx ctx;
    public MessageContextPair next = null;

    public HybridLockFreeNode node = null;
    public int threadId;

    public MessageContextPair(TOMMessage message, int classId, int index, short c1, short c2, short opId, MultiOperationCtx ctx) {
        this.request = message;
        this.classId = classId;
        this.index = index;
        this.c1 = c1;
        this.c2 = c2;
        this.opId = opId;
        this.ctx = ctx;
    }

    /*@Override
        public String toString(){
            return "request [class id= "+classId+", operation id= "+opId+" requestID: "+request+ " index: "+index+"]";
        }*/
}
