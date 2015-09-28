package DatabaseRuntimeProcessor;

import Shared.Structures.Field;
import Shared.Structures.Metadata;
import Shared.Structures.Row;
import Shared.Structures.Table;
import StoredDataManager.Main.StoredDataManager;
import SystemCatalog.Constants;
import java.util.ArrayList;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *Clase encargada de realizar el comando select
 * @author JoséAlberto
 */
public class Select {
    
    private final ArrayList<String> columns;
    StoredDataManager storer ;
    private final ArrayList<String> tables;
    private String colCond;
    private String Ope;
    private int value;
    private boolean flagAs;
    private String database;
    private Metadata met;
    
    

    /**
     * Caso de constructor para el caso de DatabaseRuntimeProcessor.Select * | columnas from tabla |
     * tablas
     *
     * @param columns
     * @param tables
     * @param Database
     */
    public Select( String Database, ArrayList<String> columns, ArrayList<String> tables) { 
        storer =  StoredDataManager.getInstance();
        storer.initStoredDataManager(Database);
        this.columns = columns;
        this.tables = tables;
        if (!columns.get(0).equals("*")){
            System.out.println("no asterisco");
            for(int i = 0; i<columns.size();i++){
                for(int j = 0; j<tables.size(); j++){
            int localCol = locCol(columns.get(i), tables.get(j));
           ArrayList<Row> filas = storer.getAllTuplesFromTable(tables.get(j));
            for (int count=0; count<filas.size();count++){
                    System.out.print(filas.get(count).getColumns().get(localCol).getContent()+"->");
                }
                    System.out.println("\n");
            
                }
            }
        }else{
            System.out.println("asterisco");
            for(int j = 0; j<tables.size(); j++){
                System.out.println("primer ciclo");
                        ArrayList<Row>  arrayRow = storer.getAllTuplesFromTable(tables.get(j));
                        System.out.println("array" + arrayRow.get(j).getTableName());
        for(int i=0;i<arrayRow.size();i++){
            System.out.println("segundo");
            ArrayList<Field> fieldList= arrayRow.get(i).getColumns();
            for(int u=0;u<fieldList.size();u++){
                System.out.print(fieldList.get(u).getContent()+" ->  ");
            }
            System.out.println();
        }

        }
        
        }     
       
    }
    
    public int locCol(String column, String Table){
        int temp = 0;
        met= storer.deserealizateMetadata();
        ArrayList<ArrayList<ArrayList<String>>> metada = met.getMetadata();
        ArrayList<ArrayList<String>> table = metada.get(Constants.COLUMNS);
            
        for (int i=0; i<table.size(); i++ ){
            
            for(int j = 0; j<table.get(i).size();i++){
                int contador = 0;
                if (table.get(i).get(Constants.TABLE_SCHNAME).equals(database) 
                        && table.get(i).get(Constants.TABLE_TABNAME).equals(Table) ){
                    if (table.get(i).get(Constants.COLUMNS_COL).equals(column)){
                        temp = contador;
                    }else{
                        contador++;
                    }
                    
                }
            }
        }
        
        return temp;
    }
//    /**
//     * Caso de constructor donde hay una condición de selección where.
//     * @param columns
//     * @param tables
//     * @param colCond
//     * @param Ope
//     * @param value 
//     */
//    public Select(ArrayList<String> columns, ArrayList<String> tables, String colCond, String Ope, int value ){
//       
//        
//    }
   
   
    //falta agregar condición para covertir el dato según el tipo de dato
    /**
     * Método de de verificar si la condición del where se va cumpliendo para 
     * cada valor de la columna.
     * @param columValue
     * @return 
     */
    public boolean verifyCond(String columValue){
        boolean result = false;
        if (Ope.equals(">")){ //>
            result = Integer.parseInt(columValue)> value;
        }
        else if (Ope.equals("<")){//<
            result = Integer.parseInt(columValue)< value;
        }
        else if (Ope.equals("=")){//=
            result = Integer.parseInt(columValue)== value;
        }
        else if (Ope.equals("like")){//like
            result=columValue.contentEquals(Ope);
            //result = Integer.parseInt(columValue)< value;
        }
        else if (Ope.equals("not")){ //not
            result = Integer.parseInt(columValue)!= value;
        }
        else if (Ope.equals("is null")){// is null
            result = columValue.equals("Null");
        }
        else if (Ope.equals("is not null")){// is not null
            result = !columValue.equals("Null");
        }
        return result;
    }
}

