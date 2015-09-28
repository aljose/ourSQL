package StoredDataManager.BPlusTree;

import java.io.Serializable;

/**
  * @author Kevin 
  * Clase encargada de crear un nodo para el árbol B+.
 */

public class NodoArbol implements Serializable {

    /**
     * Se crean los atributos de clase. NumeroLlaves describe la cantidad de
     * llaves que contiene el nodo. Llave es un arreglo con las llaves contenidas en el nodo. Objetos es un arreglo de objetos
     * que representan los valores almacenados en el nodo. NodosHijo es un arreglo de tipo StoredDataManager.BPlusTree.NodoArbol que contiene los
     * nodos existentes en el árbol. EsHoja es un valor booleano que describe si el nodo en cuestión es una hoja o es un
     * padre. NodoSiguiente es una referencia de memoria al próximo nodo del árbol. La constante T determina el grado del
     * árbol, en este caso el grado es 4..
     */
    private int numeroLlaves;
    private long[] llave;
    private Object[] objetos;
    private NodoArbol[] nodosHijo;
    private boolean esHoja;
    private NodoArbol nodoSiguiente;
    private static final int T = 4;

    public NodoArbol() {
        this.numeroLlaves = 0;
        this.llave = new long[2 * T - 1];
        this.objetos = new Object[2 * T - 1];
        this.nodosHijo = new NodoArbol[2 * T];
    }

    // Se construyen los getters y setters que permitirán obtener y establecer los valores de las atributos de clase.
    
    /**
     * Obtiene el nodo siguiente al nodo que llama a este método.
     * @return nodoSiguiente 
     */
    public NodoArbol getNodoSiguiente() {
        return nodoSiguiente;
    }
    /**
     * Establece el nodo recibido como parámetro como nodoSiguiente del nodo que llama a este método.
     * @param nodoSiguiente 
     */
    public void setNodoSiguiente(NodoArbol nodoSiguiente) {
        this.nodoSiguiente = nodoSiguiente;
    }

    /**
     * Obtiene el número de llaves contenidas en el nodo respectivo.
     * @return numeroLlaves
     */
    public int getNumeroLlaves() {
        return numeroLlaves;
    }

    /**
     * Establece, de ser necesario, un nuevo número de llaves para el nodo en cuestión.
     * @param numeroLlaves 
     */
    public void setNumeroLlaves(int numeroLlaves) {
        this.numeroLlaves = numeroLlaves;
    }
    
    /**
     *  Obtiene la llave indicada en el parámetro.
     * @param indice
     * @return llave que corresponde al índice ingresado.
     */
    public long getLlave(int indice) {
        return llave[indice];
    }

    /**
     *  Establece la llave ingresada como parámetro según el índice de entrada.
     * @param llave
     * @param indice 
     */
    public void setLlave(long llave, int indice) {
        this.llave[indice] = llave;
    }

    /**
     *  Obtiene el objeto en la posición ingresada como parámetro.
     * @param indice
     * @return Valor que corresponde al índice recibido.
     */
    public Object getObjetos(int indice) {
        return objetos[indice];
    }
    
    /**
     * Establece el valor ingresado como parámetro en el índice proporcionado. 
     * @param objeto
     * @param indice 
     */
    public void setObjeto(Object objeto, int indice) {
        this.objetos[indice] = objeto;
    }

    /**
     *  Obtiene el nodo hijo según el índice proporcionado.
     * @param indice
     * @return 
     */
    public NodoArbol getNodosHijo(int indice) {
        return nodosHijo[indice];
    }

    /**
     * Establece un nodo hijo al nodo en la posición índice ingresada como parámetro.
     * @param nodoHijo
     * @param indice 
     */
    public void setNodosHijo(NodoArbol nodoHijo, int indice) {
        this.nodosHijo[indice] = nodoHijo;
    }

    /**
     * Retorna un valor booleano si el nodo que invoca al método es un nodo hoja o si es padre.
     * @return 
     */
    public boolean isEsHoja() {
        return esHoja;
    }

    /**
     * Establece el nodo en cuestión como un nodo hoja.
     * @param esHoja 
     */
    public void setEsHoja(boolean esHoja) {
        this.esHoja = esHoja;
    }

}
