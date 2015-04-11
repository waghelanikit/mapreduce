package a1.seq.v1;

public class TestPoly {
	public static void main(String args[]) {
		
		Node n1 = new Node(1);
		Node n2 = new Node(2);
		Node n3 = new Node(3);
		Node n4 = new Node(4);
		Node n5 = new Node(5);
		Node n6 = new Node(6);
		Node n7 = new Node(7);
		BTree tree = new BTree(n1);
		n1.setLeft(n2);
		n1.setRight(n3);
		n2.setLeft(n4);
		n2.setRight(n5);
		n3.setLeft(n6);
		n3.setRight(n7);
		System.out.println("Inorder");
		tree.inOrder();
		System.out.println("\nPreorder");
		tree.preOrder();
		System.out.println("\nPostorder");
		tree.postOrder();
		System.out.println("\nLevelOrder");
		tree.levelOrder();	
		
		tree.bfs(new Node(8));
	}
	
	public static int equilibrium(int a[]){
		for(int i=0;i<a.length;i++){
			
		}
		return a[0];
	}
	public static int sum(int i,int j){
		int sum=0;
		
		return sum;
	}
}

class Node {
	int data;
	Node left;
	Node right;
	public Node() {
		data = 0;
	}
	
	public Node(int data) {
		this.data = data;
	}
	
	public void setLeft(Node v) {
		this.left = v;
	}
	
	public void setRight(Node v) {
		this.right = v;
	}
	
	public Node getLeft() {
		return left;
	}
	
	public Node getRight() {
		return right;
	}
	
	public int getData() {
		return data;
	}
	
	public String toString() {
		return ""+data;
	}
}

class BTree {
	Node root;
	
	public BTree(Node root) {
		this.root = root;
	}
	
	public BTree() {
		root = new Node();
	}
	
	public Node getRoot() {
		return root;
	}
	
	public void inOrder() {
		printInOrder(root);
	}
	
	public void preOrder() {
		printPreOrder(root);
	}
	
	public void postOrder() {
		printPostOrder(root);
	}
	
	public void levelOrder() {
		printLevelOrder(root);
	}
	
	public void printInOrder(Node root) {
		//TODO
		if(root==null)
			return;
		
		printInOrder(root.getLeft());
		System.out.print(root + ",");
		printInOrder(root.getRight());
		
		
	}
	public void printPreOrder(Node root) {
		//TODO
		if(root==null)
			return;
		System.out.print(root + ",");
		printPreOrder(root.getLeft());
		printPreOrder(root.getRight());
		
	}
	public void printPostOrder(Node root) {
		//TODO
		if(root==null)
			return;
		printPostOrder(root.getLeft());
		printPostOrder(root.getRight());
		System.out.print(root + ",");
		
	}
	public void printLevelOrder(Node root) {
		//TODO
		int h = height(root);
		if(root==null)
			return;
		for(int i=1; i<=h; i++){
			level(root, i);
		}
		
	}
	public int height(Node root){
		if(root==null)
			return 0;
		else
			return 1 + Math.max(height(root.left), height(root.right));
	}
	public void level(Node root, int l){
		if(root==null)
			return;
		if(l==1)
			System.out.println(root);
		else if(l>1){
			level(root.getLeft(),l-1);
			level(root.getRight(), l-1);
		}
	}
	
	public void bfs(Node n){
		if(n.data==root.data)
			System.out.println("Found " + n);
		else{
			int h = height(root);
			for(int i=1; i<=h; i++){
				bfsHelper(root, n, i);
				
			}
			
		}
		
	}
	
	private void bfsHelper(Node root, Node n, int l){
		if(root==null)
			return;
		if(l==1){
			if(root.data==n.data)
				System.out.println("Found " +root);
		}
		else if(l>1){
			bfsHelper(root.left, n, l-1);
			bfsHelper(root.right, n, l-1);
		}
	}
	
//	public void shuffle(Node root){
//		if(root==null)
//			return;
//		while(root!=null){
//			
//		}
//		
//	}
	
	
}