package StoredDataManager.BPlusTree;

import java.io.Serializable;


/**
 * @author Daniel Himmelein Se ha utilizado el código del URL siguiente como
 * referencia:
 * https://code.google.com/p/himmele/source/browse/trunk/Algorithms%20and%20Data%20Structures/BPlusTree/src/
 * StoredDataManager.BPlusTree.java Clase encargada de crear el árbol B+ que guarda la estructura
 * de la base de datos para posteriormente realizar el almacenamiento en disco.
 *
 */
public class ArbolBMas implements Serializable{

    /**
     * Raiz es un atributo tipo StoredDataManager.BPlusTree.NodoArbol que representa la raíz del árbol.
     */
    private NodoArbol raiz;
    private String nombreArbol;
    private static final int T = 4;

    public ArbolBMas() {
        raiz = new NodoArbol();
        raiz.setEsHoja(true);
    }

    /**
     * Método encargado de realizar el proceso de inserción de la llave y valor
     * ingresados como parámetro.
     *
     * @param llave
     * @param objeto
     */
    public void insertar(long llave, Object objeto) {

        NodoArbol nodoRaiz = raiz;
        //Condición que verifica que la llave a ingresar no esté en el árbol. Esto con el fin de evitar llaves duplicadas.
        if (search(llave) == null) {
            if (nodoRaiz.getNumeroLlaves() == (2 * T - 1)) {
                NodoArbol nuevoNodoRaiz = new NodoArbol();
                raiz = nuevoNodoRaiz;
                nuevoNodoRaiz.setEsHoja(false);
                raiz.setNodosHijo(nodoRaiz, 0);
                // Se divide el nodo raíz y se mueve la llave media al nuevo nodo raíz.
                dividirNodoHijo(nuevoNodoRaiz, 0, nodoRaiz);
                // Se inserta la nueva llave en el árbol B+ con la raíz nuevoNodoRaíz.
                insertarEnNodoNoVacio(nuevoNodoRaiz, llave, objeto);
            } else {
                //Inserta la llave en el árbol estableciendo como raíz la variable NodoRaíz.
                insertarEnNodoNoVacio(nodoRaiz, llave, objeto);
            }
        } else {
            System.out.println("No es posible duplicar una llave. La inserción no se puede ejecutar");
        }
    }

    /**
     * Divide el nodo "nodo" en dos nodos que contienen T-1 y T elementos y
     * mueve la llave media del nodo al nodo padre. El método es llamado
     * únicamente si el nodo actual está lleno. Nodo es el hijo número i del
     * nodo padre.
     *
     * @param padre
     * @param i
     * @param nodo
     */
    public void dividirNodoHijo(NodoArbol padre, int i, NodoArbol nodo) {
        NodoArbol nuevoNodo = new NodoArbol();
        nuevoNodo.setEsHoja(nodo.isEsHoja());
        nuevoNodo.setNumeroLlaves(T);
        //Copia los últimos T elementos del nodo en nuevo nodo.
        for (int j = 0; j < T; j++) {
            nuevoNodo.setLlave(nodo.getLlave(j + T - 1), j);
            nuevoNodo.setObjeto(nodo.getObjetos(j + T - 1), j);
        }
        if (!nuevoNodo.isEsHoja()) {
            //Copia las últimas T+1 referencias de nodo en nuevoNodo.
            for (int j = 0; j < T + 1; j++) {
                nuevoNodo.setNodosHijo(nodo.getNodosHijo(j + T - 1), j);
            }
            for (int j = T; j <= nodo.getNumeroLlaves(); j++) {
                nodo.setNodosHijo(null, j);
            }
        } else {
            // Gestiona la lista enlazada que es utilizada para realizar consultas rápidamente.
            nuevoNodo.setNodoSiguiente(nodo.getNodoSiguiente());
            nodo.setNodoSiguiente(nuevoNodo);
        }
        for (int j = T - 1; j < nodo.getNumeroLlaves(); j++) {
            nodo.setLlave(0, j);
            nodo.setObjeto(null, j);
        }
        nodo.setNumeroLlaves(T - 1);

        // Inserta una referencia de nodo hijo al nodo nuevoNodo en el nodo padre, trasladando las llaves y referencias 
        // según se requiera.
        for (int j = padre.getNumeroLlaves(); j >= i + 1; j--) {
            padre.setNodosHijo(padre.getNodosHijo(j), j + 1);
        }
        padre.setNodosHijo(nuevoNodo, i + 1);

        for (int j = padre.getNumeroLlaves() - 1; j >= i; j--) {
            padre.setLlave(padre.getLlave(j), j + 1);
            padre.setObjeto(padre.getObjetos(j), j + 1);
        }
        padre.setLlave(nuevoNodo.getLlave(0), i);
        padre.setObjeto(nuevoNodo.getObjetos(0), i);
        padre.setNumeroLlaves(padre.getNumeroLlaves() + 1);
    }

    /**
     * Método encargado de insertar un elemento en el árbol B+. En última
     * instancia, los valores son insertados en un nodo hoja.
     *
     * @param nodo
     * @param llave
     * @param objeto
     */
    public void insertarEnNodoNoVacio(NodoArbol nodo, long llave, Object objeto) {
        int i = nodo.getNumeroLlaves() - 1;
        if (nodo.isEsHoja()) {
            // Al no estar lleno el nodo, se inserta un nuevo elemento en el lugar correspondiente dentro del nodo.
            while (i >= 0 && llave < nodo.getLlave(i)) {
                nodo.setLlave(nodo.getLlave(i), i + 1);
                nodo.setObjeto(nodo.getObjetos(i), i + 1);
                i--;
            }
            i++;
            nodo.setLlave(llave, i);
            nodo.setObjeto(objeto, i);

            nodo.setNumeroLlaves(nodo.getNumeroLlaves() + 1);
        } else {
            //Se realiza un recorrido hacia atrás desde la última llave del nodo hasta que se encuentre una referencia de nodo
            //hijo al nodo que corresponde a la raíz del sub-árbol donde se va a ubicar el nuevo elemento. 

            while (i >= 0 && llave < nodo.getLlave(i)) {
                i--;
            }
            i++;
            if (nodo.getNodosHijo(i).getNumeroLlaves() == (2 * T - 1)) {
                dividirNodoHijo(nodo, i, nodo.getNodosHijo(i));
                if (llave > nodo.getLlave(i)) {
                    i++;
                }
            }
            insertarEnNodoNoVacio(nodo.getNodosHijo(i), llave, objeto);
        }
    }

    /**
     * Método encargado de realizar el proceso de búsqueda según un nodo de
     * referencia y la llave que se desea encontrar.
     *
     * @param nodo
     * @param llave
     * @return El valor que coincida con la llave ingresada por parámetro. Si no
     * existe en el árbol se devuelve null.
     */
    public Object buscar(NodoArbol nodo, long llave) {
        int i = 0;
        while (i < nodo.getNumeroLlaves() && llave > nodo.getLlave(i)) {
            i++;
        }
        if (i < nodo.getNumeroLlaves() && llave == nodo.getLlave(i)) {
            return nodo.getObjetos(i);
        }
        if (nodo.isEsHoja()) {
            return null;
        } else {
            return buscar(nodo.getNodosHijo(i), llave);
        }
    }

    public Object search(long llave) {
        return buscar(raiz, llave);
    }

    /**
     * Método encargado de recorrer el árbol B+ de manera que se pueda
     * visualizar su contenido de forma clara.
     *
     * @return Resultado del recorrido en ordel del árbol B+.
     */
    public String recorridoEnOrden() {
        String resultado = "";
        NodoArbol nodo = raiz;
        while (!nodo.isEsHoja()) {
            nodo = nodo.getNodosHijo(0);
        }
        while (nodo != null) {
            for (int i = 0; i < nodo.getNumeroLlaves(); i++) {
                resultado += nodo.getObjetos(i) + ", ";
            }
            nodo = nodo.getNodoSiguiente();
        }
        return resultado;
    }

    /**
     * Recorrido en orden desde una llave de inicio hasta una llave final, las
     * cuales se ingresan como parámetro.
     *
     * @param llaveInicio
     * @param llaveFinal
     * @return resultado del recorrido en orden del árbol B+.
     */
    public String recorridoEnOrden(int llaveInicio, int llaveFinal) {
        String resultado = "";
        NodoArbol nodo = obtenerHoja(llaveInicio);
        while (nodo != null) {
            for (int j = 0; j < nodo.getNumeroLlaves(); j++) {
                resultado += nodo.getObjetos(j) + ", ";
                if (nodo.getLlave(j) == llaveFinal) {
                    return resultado;
                }
            }
            nodo = nodo.getNodoSiguiente();
        }
        return resultado;
    }

    /**
     * Método encargado de obtener un nodo hoja según la llave ingresada como
     * parámetro.
     *
     * @param llave
     * @return Nodo donde se encuentra la llave proporcionada.
     */
    public NodoArbol obtenerHoja(int llave) {
        NodoArbol nodo = raiz;
        while (nodo != null) {
            int i = 0;
            while (i < nodo.getNumeroLlaves() && llave > nodo.getLlave(i)) {
                i++;
            }
            if (i < nodo.getNumeroLlaves() && llave == nodo.getLlave(i)) {
                nodo = nodo.getNodosHijo(i + 1);
                while (!nodo.isEsHoja()) {
                    nodo = nodo.getNodosHijo(i);
                }
                return nodo;
            }
            if (nodo.isEsHoja()) {
                return null;
            } else {
                nodo = nodo.getNodosHijo(i);
            }
        }
        return null;
    }

    /**
     * Método utilizado para realizar pruebas en el árbol B+.
     * @param args
     */
    public static void main(String[] args) {
        ArbolBMas bPlusTree = new ArbolBMas();
        int primeNumbers[] = new int[]{2, 3, 5, 7, 11, 13, 19, 23, 37, 41, 43, 47, 53, 59, 67, 71, 61, 73, 79, 89,
            97, 101, 103, 109, 29, 31, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 17, 83, 107};

        for (int i = 0; i < primeNumbers.length; i++) {
            bPlusTree.insertar(primeNumbers[i], String.valueOf(primeNumbers[i]));
        }

        for (int i = 0; i < primeNumbers.length; i++) {
            String value = String.valueOf(primeNumbers[i]);
            Object searchResult = (Object) bPlusTree.search(primeNumbers[i]);
            if (!value.equals(searchResult)) {
                System.out.println("Oops: Key " + primeNumbers[i] + " retrieved object " + searchResult);
            }
        }

        System.out.println(bPlusTree.search(11));
        System.out.println(bPlusTree.search(17));
        System.out.println(bPlusTree.recorridoEnOrden());
        System.out.println(bPlusTree.recorridoEnOrden(19, 71));
    }

    /**
     * @return the nombreArbol
     */
    public String getNombreArbol() {
        return nombreArbol;
    }

    /**
     * @param nombreArbol the nombreArbol to set
     */
    public void setNombreArbol(String nombreArbol) {
        this.nombreArbol = nombreArbol;
    }
}
