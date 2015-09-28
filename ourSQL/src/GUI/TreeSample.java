/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package GUI;

/**
 *
 * @author JoséAlberto
 */
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;


public class TreeSample {
    JTree tree;
  public TreeSample() {
    
    DefaultMutableTreeNode root = new DefaultMutableTreeNode("Bases de Datos");
    DefaultTreeModel modelo = new DefaultTreeModel(root);
    tree = new JTree(modelo);
    
    DefaultMutableTreeNode tabla = new DefaultMutableTreeNode("tabla");
    DefaultMutableTreeNode venus = new DefaultMutableTreeNode("Venus");
    modelo.insertNodeInto(tabla, root, 0);
    modelo.insertNodeInto(venus, root, 1);
    DefaultMutableTreeNode mars = new DefaultMutableTreeNode("Mars");
    DefaultMutableTreeNode marsa = new DefaultMutableTreeNode("Marsa");
    tabla.add(mars);
    tabla.add(marsa);
    
  }
}
