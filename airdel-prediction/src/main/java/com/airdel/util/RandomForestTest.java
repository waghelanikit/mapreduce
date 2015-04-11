package com.airdel.util;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

import com.google.common.collect.Sets;

import quickml.data.AttributesMap;
import quickml.data.Instance;
import quickml.data.InstanceImpl;
import quickml.supervised.classifier.decisionTree.Tree;
import quickml.supervised.classifier.decisionTree.TreeBuilder;

public class RandomForestTest {
	public static void main(String args[]) throws IOException {
		final Set<Instance<AttributesMap>> instances = Sets.newHashSet();
        
		BufferedReader br = new BufferedReader(new FileReader("/Users/nikit/Downloads/a3data/data.csv"));
		String line = "";
		Parser parser = new Parser(',');
		AttributesMap  attributes;
		while((line = br.readLine()) != null) {
			attributes = null; //parser.parse(line).data;
			int delayed = Math.max(parser.getInt("ArrDelay"), parser.getInt("DepDelay"));
			instances.add(new InstanceImpl<AttributesMap>(attributes, (delayed > 0)? "yes":"no"));
		}
		br.close();
		
		{
            
            TreeBuilder treeBuilder = new TreeBuilder();
            Tree tree = treeBuilder.buildPredictiveModel(instances);
            line = "2012,1,1,6,5,\"2012-01-06\",\"AA\",19805,\"AA\",\"N319AA\",1,12478,1247802,31703,\"JFK\",\"New York, NY\",\"NY\",36,\"New York\",22,12892,1289203,32575,\"LAX\",\"Los Angeles, CA\",\"CA\",6,\"California\",91,0900,0000,5,5,5,5,\"\",0,0000,0000,0,1146,0000,4,4,4,4,\"\",0,\"\",0,346,0,0,1,2475,10,0,0,0,0,0";
            
            attributes = null; //parser.parse(line).data;
            System.out.println(attributes.get("ArrDelay"));
            System.out.println(attributes.get("DepDelay"));
            
            Serializable classification = tree.getClassificationByMaxProb(attributes);
            if (classification.equals("yes")) {
                System.out.println("This flight is delayed!");
            } else  {
                System.out.println("This flight is on time!");
            } 
            tree.node.dump(System.out);
        }
	}
}
