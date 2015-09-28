package StoredDataManager.DBFile;

import java.io.IOException;
import java.io.RandomAccessFile;
/**
 * Created by manzumbado on 25/09/15.
 * Clase encargada de realizar las operaciones de lectura en el archivo de la tabla de la base de datos
 */
public class DBReader {


    private RandomAccessFile file =null;
    private boolean isInitialized =false;


    /**
     * Constructor por defecto
     */
    public DBReader() {
    }

    /**
     * Método encargado de setear el archivo al que el Reader lee
     * @param filepath String que contiene la direción del archivo en disco
     * @return 1 si realiza la operación con éxito, -1 en caso de error.
     */
    public int setTableFile(String filepath){
        try{
            this.file=new RandomAccessFile(filepath,"r");
            this.isInitialized=true;
            return 1;
        }catch(IOException ex){
            System.err.println("No se puede abrir  el archivo, error: "+ex.getMessage());
            return -1;
        }

    }

    /**
     * Método encargado de realizar la operación de lectura en el campo especificado por el offset
     * @param position Offset que indica la posición del campo al que se le quiere hacer lectura
     * @return Objeto que representa un campo en el archivo de la tabla de la base de datos, en caso
     * de error retorna null.
     */
    public DBField readFromDBFile(long position){
        int fieldSize;
        String fieldValue;
        DBField field;
        try{
            file.seek(position);
            fieldSize=file.readInt();
            fieldValue=file.readUTF();
            file.seek(file.length());
            if(fieldSize!=-1){
                field= new DBField(fieldValue,fieldSize);
            }else
                field=null;
        }catch (IOException ex){
            System.err.println("No se puede escribir en la posicion especificada:" +ex.getMessage());
            field=null;
            return field;
        }
        return field;
    }

    /**
     * Método encargado de cerrar el archivo
     * @return 1 si logra cerrar el archivo, -1 si ocurre un error.
     */
    public int closeFile(){
        try {
            this.file.close();
            return 1;
        } catch (IOException e) {
            System.err.println("Error al cerrar el archivo :" +e.getMessage());
            return -1;
        }
    }
}