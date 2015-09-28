package StoredDataManager.DBFile;

import java.io.IOException;
import java.io.RandomAccessFile;



/**
 * Created by manzumbado on 25/09/15.
 * Esta clase se encarga de realizar las operaciones de escritura en un archivo.
 */
public class DBWriter {

    private RandomAccessFile file = null;
    private boolean isInitialized=false;

    /**
     * Constructor por defecto
     */
    public DBWriter(){

    }

    /**
     * Método encargado de setear el archivo al que el Writer escribe
     * @param filepath String que contiene la direción del archivo en disco
     * @return 1 si realiza la operación con éxito, -1 en caso de error.
     */
    public int setTableFile(String filepath){
        try{
            this.file=new RandomAccessFile(filepath,"rw");
            this.isInitialized=true;
            return 1;
        }catch(IOException ex){
            System.err.println("No se puede abrir  el archivo, error: "+ex.getMessage());
            return -1;
        }
    }

    /**
     * Método encargado de realizar las operaciones de escritura en el archivo.
     * @param field Objeto DBField que representa un campo de un registro.
     * @return long que representa el offset del campo en el archivo, el cual se ingresa en el nodo
     * hoja del arbol b+. En caso de error retorna -1
     */
    public long writeToDBFile(DBField field){
        long position;
        if(this.isInitialized) {
            try {
                position = file.length();
                file.seek(position);
                file.writeInt(field.getFieldLength());
                file.writeUTF(field.getValue());
            } catch (IOException ex) {
                System.err.println("No se puede acceder al archivo, error: " + ex.getMessage());
                position = -1;
            }
        }else {
            position = -1;
        }
        return position;
    }

    /**
     * Método encargado de "eliminar" un campo en el archivo. Sobreescribe el tamaño del contenido del campo como -1, lo cual
     * significa que el archivo ha sido descartado. El campo no se elimina.
     * @param position Offset que representa la posición del campo en el archivo.
     * @return 1 si elimina con éxito, en caso de error retorna -1.
     */
    public int deleteFromDBFile(long position){
        if(this.isInitialized){
            try{
                file.seek(position);
                file.writeInt(-1);
                long lenthOfFile= file.length();
                file.seek(lenthOfFile);
                return 1;
            } catch (IOException e) {
                System.err.println("No se puede eliminar el campo, error: " +e.getMessage());
                return -1;
            }
        }else {
            return -1;
        }
    }

    /**
     * Método encargado de cerrar el archivo
     * @return 1 si logra cerrar el archivo, -1 si ocurre un error.
     */
    public  int closeFile(){
        try {
            this.file.close();
            this.file=null;
            return 1;
        } catch (IOException e) {
            System.err.println("Error al cerrar el archivo :" +e.getMessage());
            return -1;
        }
    }

}