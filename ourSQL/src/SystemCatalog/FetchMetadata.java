package SystemCatalog;

import Shared.Structures.Table;

/*
 * Clase utilizada para obtener datos de las tablas de la metadata, la metadata contiene 4 tablas, las cuales son:
 * Tabla de los esquemas de main.
 * Tabla de las tablas de todas los esquemnas de main
 * Tabla de las columnas de todas las tablas  de todos los esquemas de main.
 * Tabla del historico de consultas.
 */


/**
 *
 * @author Nicolas Jimenez
 */
public class FetchMetadata {

    /**
     *  Retorna la tabla del historico de consultas.
     * @return 
     */
    public Table fetchQueryLog() {

   //     Table temp = select("System", "QueryLog", "all");
        return null;        
    }

    /**
     *  Regresa la tabla de tablas de main.
     * @return 
     */
    public Table fetchTables() {

   //     Table temp = select("System", "Table", "all");
        return null;
    }

    /**
     * Regresa la tabla de columnas.
     * @return 
     */
    public Table  fetchColumns(  ) {
        
     //   Table temp = select ("System", "Column", "all");
        return null;
    }

    /**
     *  Regresa la tabla de las bases de datos existentes.
     * @return 
     */
    public Table fetchSchemas() {

   //     Table temp = select("System", "Schema", "all");

        return null;
    }
    
    public Table fetchForeignKey() {
        
   //     Table temp = select("System", "Schema", "all");
        
        return null;
    }
}
