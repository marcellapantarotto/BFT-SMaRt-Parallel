/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author eduardo
 */
public class MultiOperationRequest {

    public short[] id1;
    public short[] id2;
    public short opId;

    public MultiOperationRequest(int number, short opId) {
        this.id1 = new short[number];
      //  this.id2 = new short[number];
        this.opId = opId;
    }
    
      public void add(int index, short data){
        //  throw new UnsupportedOperationException();
        this.id1[index] = data;
      }

    public void add(int index, short c1, short c2){
        //this.id1[index] = c1;
        //this.id2[index] = c2;
          throw new UnsupportedOperationException();
    }
    
    public MultiOperationRequest(byte[] buffer) {
        DataInputStream dis = null;
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(buffer);
            dis = new DataInputStream(in);
            
            this.opId = dis.readShort();
            
            int size = dis.readShort();
            this.id1= new short[size];
           // this.id2= new short[size];
            
            for(int i = 0; i < this.id1.length; i++){
                this.id1[i] = dis.readShort();
                //this.id2[i] = dis.readShort();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            try {
                dis.close();
            } catch (IOException ex) {
                Logger.getLogger(MultiOperationRequest.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
		

    }

    public byte[] serialize() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream oos = new DataOutputStream(baos);
            
            oos.writeShort(opId);
            
            oos.writeShort(id1.length);
            
            for(int i = 0; i < id1.length; i++){
                oos.writeShort(this.id1[i]);
                //oos.writeShort(this.id2[i]);
            }
            oos.close();
            return baos.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

   /* public class Operation {

        public byte[] data;
        public int classId;
        public int opId;
        
        public Operation() {
        }

        public Operation(byte[] data, int classId, int opId) {
            this.data = data;
            this.classId = classId;
            this.opId = opId;
        }

        
        

    }*/

}
