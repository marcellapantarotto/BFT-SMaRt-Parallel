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

// fazer função pra ver se é uma requisição ou mais
public class MessageContextPair {
        public TOMMessage request;
        public int classId;
        public short operation;
        public int index;
        public byte[] resp;
        
        public short opId;  //vai dizer o tipo de operação que é
       
        public MultiOperationCtx ctx;
 
        
        public HybridLockFreeNode node = null;
        public int threadId;
                
        public MessageContextPair(TOMMessage message, int classId, int index, short operation, short opId, MultiOperationCtx ctx) {
            this.request = message;
            this.classId = classId;
            this.index = index;
            this.operation = operation;
            this.opId = opId;
            this.ctx = ctx;
        }
        
        @Override
        public String toString(){
            return "request [class id= "+classId+", operation id= "+opId+" requestID: "+request+ " index: "+index+"]";
        }

}
