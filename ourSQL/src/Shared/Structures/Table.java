package Shared.Structures;

import java.util.ArrayList;

/**
 * Clase encargada de crear un objeto Table el cual se genera cuando el usuario
 * define una nueva tabla.
 *
 * @author Kevin
 */
public class Table {

    /*Atributos de clase los cuales describen el nombre de la tabla, un arreglo de las filas que componen la tabla y la
     cantidad de filas existentes, respectivamente.*/
    private final String name;
    private ArrayList<Row> rows;
    private int length;
    private ArrayList<String> columnNames;
    private String primaryKey;
    private String schemaName;

    public Table(String name) {
        this.name = name;
        this.rows = null;
        this.length = 0;
    }

    public Table(String name, ArrayList<Row> rows) {
        this.name = name;
        this.rows = rows;
        this.length = rows.size();
    }

    /**
     * @return The table's name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return The table's rows.
     */
    public ArrayList<Row> getRows() {
        return rows;
    }

    /**
     * @param rows the rows to set
     */
    public void setRows(ArrayList<Row> rows) {
        this.rows = rows;
    }

    /**
     * @return the length
     */
    public int getLength() {
        return rows.size();
    }

    /**
     * @param length the length to set
     */
    public void setLength(int length) {
        this.length = length;
    }

    /**
     * @return the columnNames
     */
    public ArrayList<String> getColumnNames() {
        return columnNames;
    }

    /**
     * @param columnNames the columnNames to set
     */
    public void setColumnNames(ArrayList<String> columnNames) {
        this.columnNames = columnNames;
    }

    /**
     * @return the primaryKey
     */
    public String getPrimaryKey() {
        return primaryKey;
    }

    /**
     * @param primaryKey the primaryKey to set
     */
    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    /**
     * @return the schemaName
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @param schemaName the schemaName to set
     */
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

}
