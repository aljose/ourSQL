package SystemCatalog;

/**
 *
 * @author Kevin
 */
public final class Constants {

    public static final int SCHEMA = 0;
    public static final int TABLES = 1;
    public static final int COLUMNS = 2;
    public static final int QUERYLOG = 3;
    public static final int FOREIGNKEY = 4;
    
    //Se sigue el formato Tabla_nombreColumna
    /*Columnas Schema*/
    public static final int SCHEMA_SCHNAME = 0;
    
    /*Columnas Tables*/
    public static final int TABLE_SCHNAME = 0;
    public static final int TABLE_TABNAME = 1;
    public static final int TABLE_PK = 2;
    public static final int TABLE_FK =3;
    
 /*Columnas Columns*/
    public static final int COLUMNS_SCHNAME = 0;
    public static final int COLUMNS_TABNAME = 1;
    public static final int COLUMNS_COL= 2;
    public static final int COLUMNS_TYPE= 3;
    public static final int COLUMNS_CONSTRAINT= 4;
    public static final int COLUMNS_PK= 5;
    public static final int COLUMNS_FK= 6;
  
 /*QueryLog Columns*/
    public static final int QUERY_SCHNAME = 0;
    public static final int QUERY_TABNAME = 1;
    public static final int QUERY_PK = 2;
    
 /*Foreign Key Columns*/
    public static final int FK_SCHNAME = 0;
    public static final int FK_COLUMN = 1;
    public static final int FK_TABLE = 2;
    public static final int FK_COLREF = 3;
    public static final int FK_TABREF = 4;
   
    
    
}
