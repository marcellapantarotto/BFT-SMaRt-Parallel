/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.hybrid.early;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import parallelism.ClassToThreads;

/**
 *
 * @author eduardo
 */
public class teste {

    public static void main(String[] a) {

        // ClassToThreads[] CtoT = new EarlySchedulerMapping().generateEarly(3, 2);
        HybridClassToThreads[] CtoT = new EarlySchedulerMapping().generateMappings(3);
       // ClassToThreads[] CtoT = generate(3, 2);
        for (int i = 0; i < CtoT.length; i++) {
            System.out.println(CtoT[i]);

        }/*

        
        int[] p = new int[3];
        p[0]=0;
        p[1]=1;
        p[2]=2;
        
        System.out.println(getClassIdEarly("R", p));
        System.out.println(getClassIdEarly("W", p));*/
    }
    
    public static int getClassIdEarly(String rORw, int... partitions) {
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < partitions.length; j++) {
            //System.out.print(iv[j]);
            sb.append(partitions[j]);
        }
        sb.append(rORw);
        //System.out.print("getClassId "+sb.toString().hashCode());
        
        return sb.toString().hashCode();
    }

    public static ClassToThreads[] generate(int numPartitions, int threadsPerPartition) {

        ClassToThreads[] CtoT = null;
        int[] partitions = new int[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            partitions[i] = i;
        }

        //int[] status = new int[]{0, 1, 2, 3}; //aqui pode ser qualquer objeto que implemente Comparable
        List<SortedSet<Comparable>> allCombList = new ArrayList<SortedSet<Comparable>>(); //aqui vai ficar a resposta

        for (int nstatus : partitions) {
            allCombList.add(new TreeSet<Comparable>(Arrays.asList(nstatus))); //insiro a combinação "1 a 1" de cada item
        }

        for (int nivel = 1; nivel < partitions.length; nivel++) {
            List<SortedSet<Comparable>> statusAntes = new ArrayList<SortedSet<Comparable>>(allCombList); //crio uma cópia para poder não iterar sobre o que já foi
            for (Set<Comparable> antes : statusAntes) {
                SortedSet<Comparable> novo = new TreeSet<Comparable>(antes); //para manter ordenado os objetos dentro do set
                novo.add(partitions[nivel]);
                if (!allCombList.contains(novo)) { //testo para ver se não está repetido
                    allCombList.add(novo);
                }
            }
        }

        Collections.sort(allCombList, new Comparator<SortedSet<Comparable>>() { //aqui só para organizar a saída de modo "bonitinho"

            @Override
            public int compare(SortedSet<Comparable> o1, SortedSet<Comparable> o2) {
                int sizeComp = o1.size() - o2.size();
                if (sizeComp == 0) {
                    Iterator<Comparable> o1iIterator = o1.iterator();
                    Iterator<Comparable> o2iIterator = o2.iterator();
                    while (sizeComp == 0 && o1iIterator.hasNext()) {
                        sizeComp = o1iIterator.next().compareTo(o2iIterator.next());
                    }
                }
                return sizeComp;

            }
        });

        System.out.println(allCombList);

        int[][] threads = new int[numPartitions][threadsPerPartition];

        int tId = 0;
        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < threadsPerPartition; j++) {
                threads[i][j] = tId;
                tId++;
            }
        }

        for (int i = 0; i < numPartitions; i++) {
            String t = "Partition " + i + " : ";
            for (int j = 0; j < threadsPerPartition; j++) {
                t = t + " , " + threads[i][j];

            }
            System.out.println(t);
        }

        Iterator it = allCombList.iterator();
        while (it.hasNext()) {
            TreeSet ar = (TreeSet) it.next();
            if (ar.toArray().length > 2 && ar.toArray().length < numPartitions) {
                it.remove();
            }

        }
        System.out.println(allCombList);

        CtoT = new ClassToThreads[allCombList.size()*2];
        int posC = 0;
        for (int i = 0; i < allCombList.size(); i++) {
            Object[] ar = ((TreeSet) allCombList.get(i)).toArray();

            int[] idsW = null;
            int[] idsR = null;
            if (ar.length == 1) {
                idsW = threads[Integer.parseInt(ar[0].toString())];
                idsR = threads[Integer.parseInt(ar[0].toString())];
            } else if (ar.length == 2) {

                int[] tp1 = threads[Integer.parseInt(ar[0].toString())];
                int[] tp2 = threads[Integer.parseInt(ar[1].toString())];

                idsR = new int[2];
                idsR[0] = tp1[0];
                idsR[1] = tp2[0];

                idsW = new int[threadsPerPartition * 2];
                int pos = 0;
                for (int z = 0; z < tp1.length; z++) {
                    idsW[pos] = tp1[z];
                    pos++;
                }
                for (int z = 0; z < tp2.length; z++) {
                    idsW[pos] = tp2[z];
                    pos++;
                }
            } else {
                idsR = new int[numPartitions];
                for (int z = 0; z < threads.length; z++) {
                    idsR[z] = threads[z][0];
                }

                idsW = new int[numPartitions * threadsPerPartition];
                int pos = 0;

                for (int z = 0; z < threads.length; z++) {
                    for (int x = 0; x < threads[z].length; x++) {
                        idsW[pos] = threads[z][x];
                        pos++;
                    }
                }
            }

            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < ar.length; j++) {
                //System.out.print(iv[j]);
                sb.append(ar[j]);
            }
            sb.append("R");
            // System.out.println(sb.toString().hashCode());
            int type = ClassToThreads.CONC;
            if (ar.length > 1) {
                type = ClassToThreads.SYNC;
            }

            CtoT[posC] = new ClassToThreads(sb.toString().hashCode(), type, idsR);
            posC++;

            sb = new StringBuilder();
            for (int j = 0; j < ar.length; j++) {
                //System.out.print(iv[j]);
                sb.append(ar[j]);
            }
            sb.append("W");
            CtoT[posC] = new ClassToThreads(sb.toString().hashCode(), ClassToThreads.SYNC, idsW);
            posC++;

        }
        return CtoT;

    }
}
