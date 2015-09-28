package Shared.Structures;

import java.util.ArrayList;

/**
 * Clase encargada de crear un objeto tipo Schema el cual se define cada vez que el usuario desee generar un nuevo 
 * esquema.
 * @author Kevin
 */
public class Schema {
    
    private String name;
    private ArrayList<Table> tables;
    
    public Schema(String name){
        this.name = name;
        this.tables =null;
    }
    
    public Schema(String name, ArrayList<Table> tables){
        this.name = name;
        this.tables = tables;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the tables
     */
    public ArrayList<Table> getTables() {
        return tables;
    }

    /**
     * @param tables the tables to set
     */
    public void setTables(ArrayList<Table> tables) {
        this.tables = tables;
    }
    
    
    
}
