/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism;

import bftsmart.tom.core.messages.TOMMessage;
import java.util.LinkedList;
import java.util.List;
import parallelism.hybrid.late.HybridLockFreeNode;

/**
 *
 * @author eduardo
 */
public class MessageContextPair {

    public List<TOMMessage> request;
    public List<Integer> classId;
    public List<Integer> index;
    public List<Short> operation;
    public List<byte[]> resp;
    public List<Short> opId;  //vai dizer o tipo de operação que é
    public List<MultiOperationCtx> ctx;

    public HybridLockFreeNode node = null;
    public int threadId;

    LinkedList<MessageContextPair> list = new LinkedList();

    public MessageContextPair(List<TOMMessage> message, List<Integer> classId, List<Integer> index, List<Short> operation, List<Short> opId, List<MultiOperationCtx> ctx) {
        for (int i = 0; i < message.size(); i++) {
            this.request.add(message.get(i));
            this.classId.add(classId.get(i));
            this.index.add(index.get(i));
            this.operation.add(operation.get(i));
            this.opId.add(opId.get(i));
            this.ctx.add(ctx.get(i));
        }
    }

    @Override
    public String toString() {
        return "request [class id= " + classId + ", operation id= " + opId + " requestID: " + request + " index: " + index + "]";
    }

}
