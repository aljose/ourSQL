package DatabaseRuntimeProcessor;

import Shared.Structures.Field;
import Shared.Structures.Table;
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
    
    private final ArrayList<Table> tables;
    private String colCond;
    private String Ope;
    private int value;
    private boolean flagAs;
    
    

    /**
     * Caso de constructor para el caso de DatabaseRuntimeProcessor.Select * | columnas from tabla |
     * tablas
     *
     * @param columns
     * @param tables
     */
    public Select(ArrayList<String> columns, ArrayList<Table> tables) { //ver si recibe las tablas o los nombres
        this.columns = columns;
        this.tables = tables;

        if (columns.size() == 1 & columns.get(0).equals("*")) {
            //imprimir toda la tabla o tablas
                flagAs = true;
        } else {
        flagAs = false;
        }
        

            for (int i = 0; i < columns.size(); i++) { //navegar en las columnas 
                String tempColumn = columns.get(i);

                for (int j = 0; j < tables.size(); j++) { //navegar en las tablas
                    int countColumn = 0;

                    while (countColumn < tables.get(j).getRows().get(0).getColumns().size()) { //navegar en las columnas de las tablas

                        if (tables.get(j).getRows().get(0).getColumns()
                               .get(countColumn).getContent().equals(tempColumn)&& !flagAs) { //condicional para *

                            for (int countRow = 0; countRow < tables.get(j).getRows().size(); countRow++) { //navegar por toda la columna de una tabla en específico
                                System.out.println(tables.get(j).getRows().get(countRow).getColumns().get(countColumn)); //impresion o agreagado a tabla
                            }
                        }else{
                            for (int countRow = 0; countRow < tables.get(j).getRows().size(); countRow++) { //navegar por toda la columna de una tabla en específico
                                System.out.println(tables.get(j).getRows().get(countRow).getColumns().get(countColumn)); //impresion o agreagado a tabla
                            }
                        }
                        countColumn++;
                    }

                }
            
        }
    }
    
    /**
     * Caso de constructor donde hay una condición de selección where.
     * @param columns
     * @param tables
     * @param colCond
     * @param Ope
     * @param value 
     */
    public Select(ArrayList<String> columns, ArrayList<Table> tables, String colCond, String Ope, int value ){
        this.columns = columns;
        this.tables = tables;
        this.Ope = Ope;
        this.colCond = colCond;
        this.value = value;
        
        if (columns.size() == 1 & columns.get(0).equals("*")) {
            //imprimir toda la tabla o tablas
                flagAs = true;
        } else {
        flagAs = false;
        }
        
        int columnWhere= searchNumCol();
        for (int i = 0; i < columns.size(); i++) { //navegar en las columnas 
                String tempColumn = columns.get(i);

                for (int j = 0; j < tables.size(); j++) { //navegar en las tablas
                    int countColumn = 0;

                    while (countColumn < tables.get(j).getRows().get(0).getColumns().size()) { //navegar en las columnas de las tablas

                        if (tables.get(j).getRows().get(0).getColumns()
                                .get(countColumn).getContent().equals(tempColumn)&&!flagAs) {
                            for (int countRow = 0; countRow < tables.get(j).getRows().size(); countRow++) { //navegar por toda la columna de una tabla en específico
                                if (verifyCond(tables.get(j).getRows().get(countRow).getColumns().get(columnWhere).getContent())){//método para boolean condicion
                                                                    System.out.println(tables.get(j).getRows().get(countRow).getColumns().get(countColumn).getContent()); //impresion o agreagado a tabla
                                }
                                
                                }
                        }else{
                            for (int countRow = 0; countRow < tables.get(j).getRows().size(); countRow++) { //navegar por toda la columna de una tabla en específico
                                if (verifyCond(tables.get(j).getRows().get(countRow).getColumns().get(columnWhere).getContent())){//método para boolean condicion
                                                                    System.out.println(tables.get(j).getRows().get(countRow).getColumns().get(countColumn).getContent()); //impresion o agreagado a tabla
                                }
                                
                                }
                        }
                        countColumn++;
                    }

                }
            }
        
    }
    /**
     * Método encargado de dar el número de columna, donde se debe verificar la condición
     * @return 
     */
    public int searchNumCol() {
        int temp=-1;
        for (int j = 0; j < tables.size(); j++) { //navegar en las tablas
            int countColumn = 0;

            while (countColumn < tables.get(j).getRows().get(0).getColumns().size()) { //navegar en las columnas de las tablas

                if (tables.get(j).getRows().get(0).getColumns()
                        .get(countColumn).getContent().equals(colCond)) {
                    temp = countColumn;
                    break;
                 
                }
            }
            if (temp!= -1){
                break;
            }
        }
        return temp;
    }
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

