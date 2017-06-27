package com.xie;

import org.apache.mahout.cf.taste.common.TasteException;
//import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.*;  
import org.apache.mahout.cf.taste.impl.neighborhood.*;  
//import org.apache.mahout.cf.taste.impl.recommender.*;  
import org.apache.mahout.cf.taste.impl.similarity.*;  
import org.apache.mahout.cf.taste.model.*;   
//import org.apache.mahout.cf.taste.recommender.*;  
import org.apache.mahout.cf.taste.similarity.*;  

import java.io.*;  
import java.util.*;  
/***
 * 
 * 
 * Calculate neighborhood by CSV.
 * @author Li Tao
 *
 */
public class UserBased {

    final static int NEIGHBORHOOD_NUM = 10;
//    final static int RECOMMENDER_NUM = 3;

    public static void main(String[] args) throws IOException, TasteException {
        String file = "src/data/testCF.csv";
        
        //when duplicate, this function uses override method
        DataModel model = new FileDataModel(new File(file));
        
        //this calculate method brings problem
        UserSimilarity user = new EuclideanDistanceSimilarity(model);
        
        //fetch the matrix into neighborhood matrix
        NearestNUserNeighborhood neighbor = new NearestNUserNeighborhood(NEIGHBORHOOD_NUM, user, model);
        
//        Recommender r = new GenericUserBasedRecommender(model, neighbor, user);
//        LongPrimitiveIterator iter = model.getUserIDs();
//
//        while (iter.hasNext()) {
//            long uid = iter.nextLong();
//            List<RecommendedItem> list = r.recommend(uid, RECOMMENDER_NUM);
//            System.out.printf("uid:%s", uid);
//            for (RecommendedItem ritem : list) {
//                System.out.printf("(%s,%f)", ritem.getItemID(), ritem.getValue());
//            }
//            System.out.println();
//            List<Integer> list = neighbor.recommend(uid, RECOMMENDER_NUM);
//            System.out.printf("uid:%s", uid);
//            for (RecommendedItem ritem : list) {
//                System.out.printf("(%s,%f)", ritem.getItemID(), ritem.getValue());
//            }
//            System.out.println();
        
        System.out.println("Please input the user_id for which you want to find the neighborhood (enter 0 to exist): ");
        Scanner s = new Scanner(System.in); 
        int id=s.nextInt();
        while(id!=0){
        	long ids[]=neighbor.getUserNeighborhood(id);
        	for(int i=0; i<ids.length; i++){
        		System.out.print(ids[i]);
        		System.out.print(" Similarity:");
        		System.out.println(user.userSimilarity(id, ids[i]));
        	}
            System.out.println("Please input the user_id for which you want to find the neighborhood (enter 0 to exist): ");
            s = new Scanner(System.in); 
            id=s.nextInt();
       }
       s.close();
    }
}
