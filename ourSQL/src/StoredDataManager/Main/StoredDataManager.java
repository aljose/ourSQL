package StoredDataManager.Main; /**
 * Created by manzumbado on 22/09/15.
 */


import StoredDataManager.BPlusTree.ArbolBMas;
import StoredDataManager.DBFile.*;
import Shared.Structures.Field;
import Shared.Structures.Row;
import Shared.Structures.Metadata;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;


/**
 * Esta clase es la encargada de mantener las instancias de los arboles
 * que pertenecen a la base de datos
 */
public class StoredDataManager {

    private static StoredDataManager StoredDataManagerInstance=null;

    private HashMap<String, ArbolBMas> mHashBtrees;
    private String mCurrentDataBase;
    private boolean isInitialized = false;
    //private CacheModule=null;

    /**
     * Constante que almacena la direccion de la carpeta en el sistema donde se almacenan las bases
     * de datos
     */ 
    protected static final String DIRECTORIO_DATOS = "Databases";
    protected static final String METADATA_PATH_TO_FILE="system"+File.separator+"metadata.dat";
    protected static final String EXTENSION_ARCHIVO_TABLA =".db";
    protected static final String EXTENSION_ARCHIVO_ARBOL=".tree";
    protected static final String EXTENSION_ARCHIVO_INDICE=".index";


    protected StoredDataManager(){
        //mHashBtrees= new HashMap<String, ArbolBMas>();
    }
    
    
    public static StoredDataManager getInstance() {
      if(StoredDataManagerInstance == null) {
         StoredDataManagerInstance = new StoredDataManager();
      }
      return StoredDataManagerInstance;
   }

    /**s
     * Metodo encargado de inicializar el stored Data Manager, carga las tablas (en caso de existir) al
     * hasmap de arboles
     */
    public void initStoredDataManager(String databaseName){
        try{
            setCurrentDataBase(databaseName);
            this.mHashBtrees= new HashMap<String, ArbolBMas>();
            String[] currentBTrees = getCurrentTreeName();
            if(currentBTrees!=null){
                if(currentBTrees.length>0){
                    for(int i=0; i<currentBTrees.length; i++){
                        getmHashBtrees().put(currentBTrees[i], deserealizateBtree(currentBTrees[i]));
                    }
                }
            }
             setIsInitialized(true);
        }catch (IOException e){
            System.err.println("Error al inicializar StoredDataManager: " + e.getMessage());
        }
    }

    /**
     * Metodo encargado de establecer la carpeta de bases de datos en la que realiza acciones el StoredDataManager
     * @param currentDataBase
     */
    public void setCurrentDataBase(String currentDataBase){
        this.mCurrentDataBase=currentDataBase;
    }

    public String getmCurrentDataBase() {
        return mCurrentDataBase;
    }

    /**
     * Metodo encargado de obterner los nombres de los archivos que contirnen los arboles serializados
     * @return
     * @throws IOException
     */
    private String[] getCurrentTreeName() throws IOException {
        File directorio = new File(DIRECTORIO_DATOS + File.separator + mCurrentDataBase);
        String[] extension = new String[1];
        extension[0] = EXTENSION_ARCHIVO_ARBOL.substring(1, EXTENSION_ARCHIVO_ARBOL.length());
        Collection<File> files=FileUtils.listFiles(directorio, extension, false);
        String[] result=new String[files.size()];
        File[] fileArray=files.toArray(new File[files.size()]);
        for(int i=0;i<files.size();i++){
            result[i]=FilenameUtils.getBaseName(fileArray[i].toString());
        }
        return result;
                
                /*directorio.listFile(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                if (name.toLowerCase().endsWith(EXTENSION_ARCHIVO_ARBOL)) {
                    return true;
                } else {
                    return false;
                }
            }
        });*/
    }


    /**
     * Metodo encargado de crear el directorio de la base de datos requerida segun el nombre indicado
     *
     * @return 1 si logra crear el directorio, de otra manera retorna el codigo de error respectivo
     */
    public int createDatabase(String nombre){
        int result;
        File directorio = new File(DIRECTORIO_DATOS+File.separator+nombre);

        if(directorio.exists()){
            result= -1;
        }
        else {
            if (directorio.mkdir()) {
                result= 1;
            }else{
                System.err.println("Error al crear base de datos ");
                result= -1;
            }

    }return result;
    }


    /**
     * Metodo encargado de insertar una fila en el sistema de archivos.
     *
     * @param row
     * @return
     */
    public int insertIntoTable(Row row){
        int result=-1;
        if(getisInitialized()){
            String targetTable= row.getTableName();
            ArrayList<Field> fields = row.getColumns();
            DBField dbField;
            ArbolBMas Btree;
            LinkedHashMap<String,Long> keyHash;
            String rowPKValue=null;
            long rowPKIndex;
            long lastRowPKIndex;
            String valueToInsert;
            try{
                DBWriter writer= new DBWriter();
                writer.setTableFile(DIRECTORIO_DATOS + File.separator + getmCurrentDataBase() + File.separator + targetTable + EXTENSION_ARCHIVO_TABLA);
                keyHash= deserializateIndex(targetTable);
                int keyHashSize=keyHash.size();
                if(keyHashSize>0){
                    lastRowPKIndex=keyHashSize;
                }else{
                    lastRowPKIndex=0;
                }
                long[] offsets= new long[fields.size()-1];
                if(this.getmHashBtrees().containsKey(targetTable)){
                    Btree=this.getmHashBtrees().get(targetTable);
                }else{
                    System.err.println("Error al ingresar datos, la tabla no existe");
                    return -1;
                }
                for(int i=0; i<fields.size();i++){
                    if(fields.get(i).isPrimaryKey()){
                        rowPKValue=fields.get(i).getContent();
                        keyHash.put(rowPKValue,lastRowPKIndex);
                        lastRowPKIndex++;
                    }else{
                        valueToInsert= fields.get(i).getContent();
                        dbField= new DBField(valueToInsert, valueToInsert.getBytes().length);
                        offsets[i-1]= writer.writeToDBFile(dbField);
                    }
                }
                writer.closeFile();
                Btree.insertar(keyHash.get(rowPKValue), offsets);
                serializateIndex(keyHash,  targetTable);
                serializateBtree(getmHashBtrees().get(targetTable), targetTable); //Recordar pasar esto al metodo flushToDisk
                result= 1;
            }catch(Exception ex){
                System.err.println("Ha ocurrido un problema al ingresar datos, error: " +ex.getMessage());
                result= -1;
            }
        }
        return result;
    }

    /**
     * Metodo encargado de eliminar la tabla, eliminando el archivo, el inidice y el arbol.
     * @param name
     * @return 
     */
    public int dropTable(String name) {
        int result=0;
        if (getisInitialized()){
            File archivoArbol = new File(DIRECTORIO_DATOS+File.separator+getmCurrentDataBase()+File.separator+name+EXTENSION_ARCHIVO_ARBOL);
            File archivoDatos = new File(DIRECTORIO_DATOS+File.separator+getmCurrentDataBase()+File.separator+name+EXTENSION_ARCHIVO_TABLA);
            File archivoIndex = new File(DIRECTORIO_DATOS+File.separator+getmCurrentDataBase()+File.separator+name+EXTENSION_ARCHIVO_INDICE);
            if(archivoArbol.delete()){
                result= 1;
            }
            if(archivoDatos.delete()){
                result=1;
            }if(archivoIndex.delete()) {
                result = 1;
            }
        }
        return result;
    }


/**
 * Metodo encargado de borar la carpeta de la base de datos, en caso de no ser la 
 * base de datos qinstanciada en el momento
 * @param name
 * @return 
 */
    public int dropDatabase(String name){
        int result;
        if(!name.equals(this.getmCurrentDataBase())){
            try{
                File directorio = new File(DIRECTORIO_DATOS+File.separator+name);

                if(directorio.exists()){
                    File[] currentFiles = directorio.listFiles();
                    for (int i=0;i<currentFiles.length;i++){
                        currentFiles[i].delete();
                    }
                    directorio.delete();
                    result=1;
                }
                else {
                    System.err.println("Error al borrar base de datos ");
                    result= -1;
                }
            }catch(Exception ex){
                result =-1;
            }
        }else{
            System.err.println("Error al borrar base de datos ");
            result= -1;
        }
        return result;
    }


    /**
     * Metodo encargado de serializar el arbol en disco
     * @param Btree
     * @param pathFile
     * @return 
     */
    private int serializateBtree(ArbolBMas Btree, String name){
        try{
            FileOutputStream outputFile= new FileOutputStream(DIRECTORIO_DATOS+File.separator+getmCurrentDataBase()+File.separator+name+EXTENSION_ARCHIVO_ARBOL);
            ObjectOutputStream outputStream = new ObjectOutputStream(outputFile);
            outputStream.writeObject(Btree);
            outputStream.close();
            outputFile.close();
            return 1;
        } catch(IOException e){
            System.err.println("No se ha podido guardar el arbol en disco, error: " + e.getMessage());
            return -1;
        }
    }

    /**
     * Metodo encargado de deserializar el arbol del disco
     * @param filepath nombre de la tabla a la que pertenece el arbol
     * @return ArbolBmas
     */
    private ArbolBMas deserealizateBtree(String name){
        ArbolBMas deserializedBtree=null;
        try{
            FileInputStream inputFile= new FileInputStream(DIRECTORIO_DATOS+File.separator+getmCurrentDataBase()+File.separator+name+EXTENSION_ARCHIVO_ARBOL);
            ObjectInputStream inputStream = new ObjectInputStream(inputFile);
            deserializedBtree= (ArbolBMas) inputStream.readObject();
            inputStream.close();
            inputFile.close();
        } catch (IOException e) {
            System.err.println("No se ha podido deserealizar el arbol, error: "+ e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return deserializedBtree;
    }

    /**
     *
     * @param hashkey
     * @param pathfile
     * @return
     */
    private int serializateIndex(LinkedHashMap<String,Long> hashkey, String name){
        try{
            FileOutputStream outputFile= new FileOutputStream(DIRECTORIO_DATOS+File.separator+getmCurrentDataBase()+File.separator+name+EXTENSION_ARCHIVO_INDICE);
            ObjectOutputStream outputStream = new ObjectOutputStream(outputFile);
            outputStream.writeObject(hashkey);
            outputStream.close();
            outputFile.close();
            return 1;
        } catch(IOException e){
            System.err.println("No se ha podido guardar el 'indice en disco, error: " + e.getMessage());
            return -1;
        }
    }

    /**
     * Metodo encargado de deserializar un indice almacenado en disco, segun el nombre
     * @param filepath ubicacion y nombre del archivo
     * @return Carga el hash que almacena el indice
     */
    private LinkedHashMap<String,Long> deserializateIndex(String name){
        LinkedHashMap<String,Long> deserializedBtree=null;
        try{
            FileInputStream inputFile= new FileInputStream(DIRECTORIO_DATOS+File.separator+getmCurrentDataBase()+File.separator+name+EXTENSION_ARCHIVO_INDICE);
            ObjectInputStream inputStream = new ObjectInputStream(inputFile);
            deserializedBtree= (LinkedHashMap<String,Long>) inputStream.readObject();
            inputStream.close();
            inputFile.close();
        } catch (IOException e) {
            System.err.println("No se ha podido deserealizar el indice, error: "+ e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return deserializedBtree;
    }

    /**
     * Metodo encargado de escribir en disco el contenido de los arboles
     * @return
     */
    public int flushToDisk(){
        try{
            String[] listaTablas= getNombreTablas();
            for(int i=0; i<listaTablas.length;i++){
                serializateBtree(getmHashBtrees().get(listaTablas[i]), listaTablas[i]);
            }
            return 1;
        } catch(IOException e){
            System.err.println("No se ha podido guardar el arbol en disco, error: " + e.getMessage());
            return -1;
        }
    }




    /**
     * Metodo encargado de obtener los nombres de las tablas para el directorio actual de la base de datos
     * @return Arreglo de objetos File que representan las tablas existentes
     */
    private String[] getNombreTablas() throws IOException{
            File directorio = new File(DIRECTORIO_DATOS + File.separator + mCurrentDataBase);
            String[] extension = new String[1];
        extension[0] = EXTENSION_ARCHIVO_TABLA.substring(1, EXTENSION_ARCHIVO_TABLA.length());
        Collection<File> files=FileUtils.listFiles(directorio, extension, false);
        String[] result=new String[files.size()];
        File[] fileArray=files.toArray(new File[files.size()]);
        for(int i=0;i<files.size();i++){
            result[i]=FilenameUtils.getBaseName(fileArray[i].toString());
        }
        return result;
    }

    
    /**
     * Devuelve el hash de los arboles de las tablas de la base de datos instanciada
     * @return 
     */
    public HashMap<String, ArbolBMas> getmHashBtrees() {
        return mHashBtrees;
    }

    
    
   

    /**
     * Metodo encargado de crear un archivo en blanco que almacena los campos de la tabla. Ademas crea e 
     * introduce el arbol correspondiente al hash de arboles y crea el indice para la llave primaria de la
     * tabla
     * 
     */

    public int createTableFile(String name){
        int result = 0;
        ArbolBMas Btree= new ArbolBMas();
        LinkedHashMap<String,Long> hashKeys= new LinkedHashMap<String, Long>();
        if(getisInitialized()){
            try{
                RandomAccessFile file= new RandomAccessFile(DIRECTORIO_DATOS+File.separator+mCurrentDataBase+File.separator+name+EXTENSION_ARCHIVO_TABLA, "rw");
                file.close();
                Btree.setNombreArbol(name);
                this.getmHashBtrees().put(name,Btree);
                serializateIndex(hashKeys,name);
                result=1;
            } catch (IOException exc){
                System.err.println("No se ha podido crear la tabla "+name+", error: "+ exc.getMessage());
                result =-1;
            }
        }else
        result = -1;
    return result;
    }


    /**
     * Metodo que obtiene el booleano que indica si el StoredDataManager se encuentra inicializado
     * @return true si est'a inicializado, false si no lo esta
     */
    public boolean getisInitialized() {
        return this.isInitialized;
    }

    /**
     * Metodo que setea un booleano cuando si el  StoredDataManager se encuentra inicializado
     * @param isInitialized 
     */ 
    private void setIsInitialized(boolean isInitialized) {
        this.isInitialized = isInitialized;
    }

    
    /**
     * Metodo encargado de escribir en disco la metadata
     * @param metadata Objeto que representa la metadata
     * @return 1 en caso de exito, -1 si ocurre un error.
     */
    public int serializeMetadata(Metadata metadata){
        try{
            FileOutputStream outputFile= new FileOutputStream(METADATA_PATH_TO_FILE);
            ObjectOutputStream outputStream = new ObjectOutputStream(outputFile);
            outputStream.writeObject(metadata);
            outputStream.close();
            outputFile.close();
            return 1;
        } catch(IOException e){
            System.err.println("No se ha podido guardar el metadata en disco, error: " + e.getMessage());
            return -1;
        }
    }
        
        
    public Metadata deserealizateMetadata(){
        Metadata deserializedMetadata=null;
        try{
            FileInputStream inputFile= new FileInputStream(METADATA_PATH_TO_FILE);
            ObjectInputStream inputStream = new ObjectInputStream(inputFile);
            deserializedMetadata= (Metadata) inputStream.readObject();
            inputStream.close();
            inputFile.close();
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("No se ha podido deserealizar el arbol, error: "+ e.getMessage());
        }
        return deserializedMetadata;
    }
        
    
    
        /**
         * Metodo encargado de calcular el numero de tuplas que existen en una tabla
         * @param tableName Nombre de la tabla
         * @return numero de registros.
         */
        public int numberOfRecords(String tableName){
           int numberRecords=0;
           if(getisInitialized()){
               LinkedHashMap<String,Long> keyHash= deserializateIndex(tableName);
               if(keyHash!=null)
               numberRecords=keyHash.size();
               else
                   return -1;
           }else{
               System.err.println("Necesita inicializar el StoredDataManager " );
           }
           return numberRecords;
        }
        
        /**Metodo encargado de crear un indice sobre el campo de la tabla especificado
         * 
         * @param tableName
         * @param indexName
         * @param columnNum
         * @return 
         */
        public int createIndex(String tableName, String indexName, int columnNum){
            int result;
                if(getisInitialized()){
                    int numberRecords= numberOfRecords(tableName);
                    ArbolBMas Btree = getmHashBtrees().get(tableName);
                    LinkedHashMap<String,Long> indexHash= new LinkedHashMap<String,Long>();
                    DBReader reader= new DBReader();
                    reader.setTableFile(DIRECTORIO_DATOS+File.separator+getmCurrentDataBase()+File.separator+tableName+EXTENSION_ARCHIVO_TABLA);
                    DBField field;
                    for(long i=0; i<numberRecords; i++){
                        long[] arrayLong=(long[]) Btree.search(i);
                        field=reader.readFromDBFile(arrayLong[columnNum-2]);
                        indexHash.put(field.getValue(), i);
                    }
                    reader.closeFile();
                    result=serializateIndex(indexHash, indexName);
               }else{
               System.err.println("Necesita inicializar el StoredDataManager " );
               result=-1;
               }
            return result;
        }
        
        /**
         * Metodo encargado de borrar el indice indicado
         * @param indexName
         * @return 
         */
        public int deleteIndex(String indexName){
             int result=0;
                if(getisInitialized()){
                    File archivoIndex = new File(DIRECTORIO_DATOS+File.separator+getmCurrentDataBase()+File.separator+indexName+EXTENSION_ARCHIVO_INDICE);
                    if(archivoIndex.delete()) {
                        result = 1;
                    }
               }else{
               System.err.println("Necesita inicializar el StoredDataManager " );
               result=-1;
               }
            return result;
        }
        
        /**
         * Metodo encargado de devolver todas las filas de una tabla dada
         * @param tableName
         * @return 
         */
        public ArrayList<Row> getAllTuplesFromTable(String tableName){
            ArrayList<Row> rowList=new ArrayList<Row>();
            if(getisInitialized()){
                int numberRecords= numberOfRecords(tableName);
                ArbolBMas Btree = getmHashBtrees().get(tableName);
                LinkedHashMap<String,Long> keyHash= deserializateIndex(tableName);
                DBReader reader= new DBReader();
                reader.setTableFile(DIRECTORIO_DATOS+File.separator+getmCurrentDataBase()+File.separator+tableName+EXTENSION_ARCHIVO_TABLA);
                
                for(long i=0; i<numberRecords; i++){
                    Row fila=new Row();
                    ArrayList<Field> fieldList= new ArrayList<Field>();
                    long[] arrayLong=(long[]) Btree.search(i);
                    String pkValue = null;
                    for (Map.Entry<String, Long> entry : keyHash.entrySet()) {
                        if(entry.getValue().equals(i)){
                            pkValue=entry.getKey();
                        }
                    }
                    if(pkValue==null){
                        numberRecords++;
                        continue;
                    }
                    Field pkField=new Field(pkValue,"",false,tableName,this.getmCurrentDataBase(),true);
                    fieldList.add(pkField);
                    for(int u=0; u<arrayLong.length;u++){
                        DBField dataFilefield= reader.readFromDBFile(arrayLong[u]);
                        if(dataFilefield!=null){
                            Field dataField= new Field(dataFilefield.getValue(),"",false,tableName,this.getmCurrentDataBase(),false);
                            fieldList.add(dataField);
                        }else{
                            System.err.println("ha ocurrido un error al obtener los campos desde el hdd" );
                            return null;
                        }
                    }
                    fila.setColumns(fieldList);
                    rowList.add(fila);
                }
                reader.closeFile();
                return rowList;
            }else{
            System.err.println("Necesita inicializar el StoredDataManager " );
            return null;
               }
        }
        
        
        public int deleteRow(String rowPKValue, String targetTable){
            int result=-1;
            if(getisInitialized()){
                ArbolBMas Btree;
                LinkedHashMap<String,Long> keyHash;
                try{
                    DBWriter writer= new DBWriter();
                    writer.setTableFile(DIRECTORIO_DATOS + File.separator + getmCurrentDataBase() + File.separator + targetTable + EXTENSION_ARCHIVO_TABLA);
                    keyHash= deserializateIndex(targetTable);
                    if(this.getmHashBtrees().containsKey(targetTable)){
                        Btree=this.getmHashBtrees().get(targetTable);
                        long[] fileFieldsPointers =(long[])Btree.search(keyHash.get(rowPKValue));
                        for(int i=0;i<fileFieldsPointers.length;i++){
                            result=writer.deleteFromDBFile(fileFieldsPointers[i]);
                        }
                        writer.closeFile();
                        //Btree.eliminar(keyHash.get(rowPKValue));
                        keyHash.remove(rowPKValue);
                    }else{
                        System.err.println("Error al eliminar la fila, la tabla no existe");
                        return -1;
                    }
                    serializateIndex(keyHash,  targetTable);
                    serializateBtree(getmHashBtrees().get(targetTable), targetTable);
                    result= 1;
                }catch(Exception ex){
                    System.err.println("Ha ocurrido un problema al eliminar la fila, error: " +ex.getMessage());
                    result= -1;
                }
            }
            return result;
        }
        /*
        
        public updateTuple(String pkValue, Row rowToUpdate){
            if(getisInitialized()){
                String targetTable= rowToUpdate.getTableName();
                int numberRecords= numberOfRecords(targetTable);
                ArbolBMas Btree = getmHashBtrees().get(targetTable);
                LinkedHashMap<String,Long> keyHash= deserializateIndex(targetTable);
                DBReader reader= new DBReader();
                reader.setTableFile(DIRECTORIO_DATOS+File.separator+getmCurrentDataBase()+File.separator+targetTable+EXTENSION_ARCHIVO_TABLA);
                
                for(long i=0; i<numberRecords; i++){
                    Row fila=new Row();
                    ArrayList<Field> fieldList= new ArrayList<Field>();
                    long[] arrayLong=(long[]) Btree.search(i);
                    String pkValue = null;
                    for (Map.Entry<String, Long> entry : keyHash.entrySet()) {
                        if(entry.getValue().equals(i)){
                            pkValue=entry.getKey();
                        }
                    }
                    if(pkValue==null){
                        numberRecords++;
                        continue;
                    }
                    Field pkField=new Field(pkValue,"",false,tableName,this.getmCurrentDataBase(),true);
                    fieldList.add(pkField);
                    for(int u=0; u<arrayLong.length;u++){
                        DBField dataFilefield= reader.readFromDBFile(arrayLong[u]);
                        if(dataFilefield!=null){
                            Field dataField= new Field(dataFilefield.getValue(),"",false,tableName,this.getmCurrentDataBase(),false);
                            fieldList.add(dataField);
                        }else{
                            System.err.println("ha ocurrido un error al obtener los campos desde el hdd" );
                            return null;
                        }
                    }
                    fila.setColumns(fieldList);
                    rowList.add(fila);
                }
                reader.closeFile();
                return rowList;
            }else{
            System.err.println("Necesita inicializar el StoredDataManager " );
            return null;
               }
        }
        
        */
         
}