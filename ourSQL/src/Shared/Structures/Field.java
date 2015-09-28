package Shared.Structures;

/**
 * Clase encargada de guardar el contenido de una columna específica. Posee
 * atributos que describen nombre de la columna, tipo de dato que almacena,
 * tabla al que pertenece, esquema al que pertenece , constraint que posee, y si
 * el campo es llave primaria.
 *
 * @author Kevin
 */
public class Field {

    private String content;
    private String type;
    private String tableName;
    private String schemaName;
    private boolean isNull;
    private boolean primaryKey;

    public Field(String content, String type, boolean isNull, boolean primaryKey) {
        this.content = content;
        this.type = type;
        this.isNull = isNull;
        this.primaryKey = primaryKey;
    }

    public Field(String content, String type, boolean isNull, String tableName, String schemaName, boolean primaryKey) {
        this.content = content;
        this.type = type;
        this.isNull = isNull;
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.primaryKey = primaryKey;
    }
    public Field(){
        this.content = "";
        this.type = "";
        this.isNull = false;
        this.tableName = "";
        this.schemaName = "";
        this.primaryKey = false;
    }

    /**
     * @return the content
     */
    public String getContent() {
        return content;
    }

    /**
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @return the schemaName
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @return the isNull
     */
    public boolean getIsNull() {
        return isNull;
    }

    /**
     * @return the primaryKey
     */
    public boolean isPrimaryKey() {
        return primaryKey;
    }

    /**
     * @param content the content to set
     */
    public void setContent(String content) {
        this.content = content;
    }

    /**
     * @param type the type to set
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * @param tableName the tableName to set
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * @param schemaName the schemaName to set
     */
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @param isNull the isNull to set
     */
    public void setIsNull(boolean isNull) {
        this.isNull = isNull;
    }

    /**
     * @param primaryKey the primaryKey to set
     */
    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }
    
}
