package DatabaseRuntimeProcessor;

import Shared.Structures.Table;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author nicolasjimenez
 */
public class Update {
    
    private final String columns;
    
    private final Table tables;
    private String colCond;
    private String Ope;
    private String newValue;
    private int value;
    
    /**
     * Constructor para el comando update con el caso del where
     * @param columns
     * @param tables
     * @param newValue
     * @param colCond
     * @param Ope
     * @param value 
     */
    public Update(String columns, Table tables,String newValue, String colCond, String Ope, int value){// caso con where
        this.columns = columns;
        this.tables = tables;
        this.Ope = Ope;
        this.colCond = colCond;
        this.value = value;
        this.newValue = newValue;
                    int countColumn = 0;
int columnWhere= searchNumCol();
                    while (countColumn < tables.getRows().get(0).getColumns().size()) { //navegar en las columnas de las tablas

                        if (tables.getRows().get(0).getColumns()
                               .get(countColumn).getContent().equals(columns)) { //condicional para *

                            for (int countRow = 0; countRow < tables.getRows().size(); countRow++) { //navegar por toda la columna de una tabla en específico
                                if (verifyCond(tables.getRows().get(countRow).getColumns().get(columnWhere).getContent())){//método para boolean condicion
                                    tables.getRows().get(countRow).getColumns().get(countColumn).getContent();//falta para modificar
                                                                    System.out.println(tables.getRows().get(countRow).getColumns().get(countColumn).getContent()); //impresion o agreagado a tabla
                                }
                                }
                        }
                        countColumn++;
                    }
    }
    
    /**
     * Método encargado de dar el número de columna, donde se debe verificar la condición
     * @return 
     */
             public int searchNumCol() {
        int temp=-1;
            int countColumn = 0;

            while (countColumn < tables.getRows().get(0).getColumns().size()) { //navegar en las columnas de las tablas

                if (tables.getRows().get(0).getColumns()
                        .get(countColumn).getContent().equals(colCond)) {
                    temp = countColumn;
                    break;
                 
                }
                countColumn++;
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
