package Analysis;

import DatabaseRuntimeProcessor.CreateDatabase;
import DatabaseRuntimeProcessor.CreateTable;
import DatabaseRuntimeProcessor.MightyMain;
import Shared.Structures.Field;
import Shared.Structures.Row;
import java.util.ArrayList;

/**
 * Clase para analizar cada instruccion entrante
 *
 * @author nicolasjimenez
 */
public class Parser {
MightyMain mm = new MightyMain();
    private final ArrayList<String> comparators;
    private final ArrayList<String> aggregate;

    /**
     *
     */
    public Parser() {

        comparators = new ArrayList();           // Comparadores 
        comparators.add("<");
        comparators.add(">");
        comparators.add("=");
        comparators.add("like");
        comparators.add("not");

        this.aggregate = new ArrayList();  // Funciones agregadas 
        aggregate.add("count");
        aggregate.add("average");
        aggregate.add("min");
        aggregate.add("max");
    }

    /**
     * Llama a whatOperation para realizar el analisis.
     *
     * @param instruction
     * @param databaseName
     * @return
     */
    public boolean parse(ArrayList<String> instruction) {

        return whatOperation(instruction);
    }

    /**
     * Ubica cada instruccion en su correspondiente metodo axuiliar para
     * resolverlo A menos de que sea muy trivial, entonces se comprueba aqui
     * mismo.
     *
     * @return
     */
    private boolean whatOperation(ArrayList<String> instruction) {

        int instructionSize = instruction.size();

        if (instructionSize == 0) {
            return false;
        }

        String token0 = instruction.get(0);

        if (instruction.size() == 1) {
            return token0.equalsIgnoreCase("start") || token0.equalsIgnoreCase("stop");
        }

        String token1 = instruction.get(1);

        if (token0.equalsIgnoreCase("create")) {

            if (token1.equalsIgnoreCase("database")) {

                if (!(instructionSize == 3)) {
                    return false;
                }

                if (!isNumeric(instruction.get(2))) {
                      mm.processer(instruction);
                    return true;
                }
                return false;

            } else if (token1.equalsIgnoreCase("table")) {

                if(createTable(instruction, instructionSize)){
                    mm.processer(instruction);
                    return true;
                }else{
                    return false;
                }

            } else if (token1.equalsIgnoreCase("index")) {

                return createIndex(instruction, instructionSize);
            } else {
                return false;
            }
        }
        if (token0.equalsIgnoreCase("drop")) {

            return drop(instruction, instructionSize);
        }
        if (token0.equalsIgnoreCase("list")) {

            return token1.equalsIgnoreCase("databases") && instructionSize == 2;
        }
        if (token0.equalsIgnoreCase("get")) {

            return token1.equalsIgnoreCase("status") && instructionSize == 2;
        }
        if (token0.equalsIgnoreCase("display")) {

            if (token1.equalsIgnoreCase("database") && instructionSize == 3) {

                if (!isNumeric(instruction.get(2))) {
                    return true;
                }
            }
            return false;
        }
        if (token0.equalsIgnoreCase("set")) {

            if (token1.equalsIgnoreCase("database") && instructionSize == 3) {

                if (!isNumeric(instruction.get(2))) {
                    return true;
                }
            }
            return false;
        }
        if (token0.equalsIgnoreCase("alter")) {

            return alter(instruction, instructionSize);
        }
        if (token0.equalsIgnoreCase("select")) {

            return select(instruction, instructionSize);
        }
        if (token0.equalsIgnoreCase("update")) {

            return update(instruction, instructionSize);
        }
        if (token0.equalsIgnoreCase("delete")) {

            return delete(instruction, instructionSize);
        }
        if (token0.equalsIgnoreCase("insert")) {
            mm.processer(instruction);
            return insert(instruction, instructionSize);
        } else {
            return false;
        }
    }

    /**
     * Metodo que analiza el drop, tanto si es de table como de database
     *
     * @param size
     * @return
     */
    private boolean drop(ArrayList<String> instruction, int size) {

        if (size != 3) {
            return false;
        }
        String token1 = instruction.get(1);
        if (token1.equalsIgnoreCase("table") || token1.equalsIgnoreCase("database")) {
                
              mm.processer(instruction);
         
            return !isNumeric(instruction.get(2));

        } else {
            return false;
        }

    }

    /**
     * Metodo para analizar el create Table
     *
     * @param size
     * @return
     */
    private boolean createTable(ArrayList<String> instruction, int size) {

        if (size < 15) {
            return false;
        }
        if (!instruction.get(1).equalsIgnoreCase("table") || isNumeric(instruction.get(2))
                || !instruction.get(3).equalsIgnoreCase("as") || !instruction.get(4).equals("(")) {
            return false;
        }
        int continuar = columnDefinition(instruction, 5, size, "primary");  // El metodo column Definition revisa que las columnas declaras
        if (continuar == -1) {                                                   // Esten correctas   
            return false;
        }
        if (continuar + 8 > size) {
            return false;
        }
        if (!instruction.get(continuar + 1).equalsIgnoreCase("primary")
                || !instruction.get(continuar + 2).equalsIgnoreCase("key") || !instruction.get(continuar + 3).equals("(")
                || isNumeric(instruction.get(continuar + 4)) || !instruction.get(continuar + 5).equals(")")) {
            return false;
        }
        return instruction.get(continuar + 6).equals(")") || instruction.get(continuar + 7).equals(";");//            createTab.createTable(databaseName, instruction.get(2), columnas );
    }

    /**
     * El metodo createIndex revisa que esta sentencia este bien formulada,
     * siguiendo el tamano y los estandares correspondientes
     *
     * @param size
     * @return
     */
    private boolean createIndex(ArrayList<String> instruction, int size) {

        if (instruction.size() != 8) {
            return false;
        }
        String tableName = instruction.get(4);
        String columnName = instruction.get(6);
        return instruction.get(3).equalsIgnoreCase("on") && instruction.get(5).equals("(")
                && instruction.get(7).equals(")") && !isNumeric(tableName) && !isNumeric(columnName);
    }

    /**
     * El alter revisa la sintaxis de un alter table, que cumpla longitud y
     * parametros establecidos
     *
     * @param size
     * @return
     */
    private boolean alter(ArrayList<String> instruction, int size) {

        if (size != 15) {
            return false;
        }

        if (isNumeric(instruction.get(2)) || isNumeric(instruction.get(8)) || isNumeric(instruction.get(11))
                || isNumeric(instruction.get(13))) {
            return false;
        }

        return instruction.get(1).equalsIgnoreCase("table") && instruction.get(3).equalsIgnoreCase("add")
                && instruction.get(4).equalsIgnoreCase("constraint") && instruction.get(5).equalsIgnoreCase("foreign")
                && instruction.get(6).equalsIgnoreCase("key") && instruction.get(7).equals("(")
                && instruction.get(9).equals(")") && instruction.get(10).equalsIgnoreCase("references")
                && instruction.get(12).equals("(") && instruction.get(14).equals(")");
    }

    /**
     * Select procesa la sentencia select, revisando las columnas, (* La unica
     * funcion agregada (si hay)) La tabla o el join de tablas ( si se
     * selecciona una sola tabla no puede haber joins) Sino solo una tabla es
     * valido El where statement si hay, la cual es una columna comparada con un
     * valor o con un operador unario El group si hay es para una lista de
     * columnas For Json o For XML (si hay, no se pueden tener los dos al mismo
     * tiempo )
     *
     * @param size
     * @return
     */
    private boolean select(ArrayList<String> instruction, int size) {

        if (size < 4) {
            return false;
        }
        int continuar = columns(instruction, 1, size, "from"); // Revisa que las columnas esten bien puestas

        if (continuar == -1 || !(size > continuar + 1)) {
            return false;
        }
        continuar = join(instruction, continuar + 1, size);// revisa la sentencia join o la unica tabla

        if (continuar == 0) {
            return true;
        }
        if (continuar == -1) {
            return false;
        }
        String instruccion = instruction.get(continuar);
        if (instruccion.equalsIgnoreCase("where")) {

            if (size <= continuar + 3) {
                return false;
            }
            if (size > continuar + 4) {

                if (instruction.get(continuar + 4).equalsIgnoreCase("null")) {

                    if (size > continuar + 5) {

                        continuar = whereStatement(instruction, continuar, continuar + 5, continuar + 4);  // si termina el where en un is not null
                        if (continuar == -1) {                                                                          // entonces se sigue

                            return false;
                        }
                        instruccion = instruction.get(continuar);

                    } else if (size == continuar + 5) {                       // si termina el where con un is not null

                        return whereStatement(instruction, continuar, size, continuar + 4) != -1;
                    } else {
                        return false;
                    }

                } else if (instruction.get(continuar + 4).equalsIgnoreCase("group") //ehereefefefef****
                        || instruction.get(continuar + 4).equalsIgnoreCase("for")) {

                    continuar = whereStatement(instruction, continuar, continuar + 4, continuar + 4); // si de fijo sigue y no hay is not null
                    if (continuar == -1) {
                        return false;
                    }
                    instruccion = instruction.get(continuar);
                } else {
                    return false;
                }
            } else if (size == continuar + 4) {

                return whereStatement(instruction, continuar, size, continuar + 4) != -1;  //si ya es el fin un where sin (not null)
            }           // El where revisa el where si existe
        }

        if (instruccion.equalsIgnoreCase("group")) {

            if (!instruction.get(continuar + 1).equalsIgnoreCase("by") || size <= continuar + 2) {
                return false;
            }
            String token;
            boolean last = false;  // false si es una ","  ;  si es una columna: true
            for (int i = continuar + 2; i < size; i++) {

                token = instruction.get(i);

                if (token.equalsIgnoreCase("for")) {
                    continuar = i;
                    instruccion = instruction.get(continuar);
                    break;
                }
                if (i + 1 == size && !token.equals(",")) {
                    return !last;
                }
                if (last) {
                    //last es true; significa que debemos encontrar una , 
                    if (!token.equals(",")) {
                        return false;
                    }
                    last = false;
                } else {   // last es false; significa que debemos encontrar una columna 

                    if (token.equals(",") || isNumeric(token)) {
                        return false;
                    }
                    last = true;
                }
            }
        }
        if (instruccion.equalsIgnoreCase("for")) {  // si es un for, o xml o json

            return (size == continuar + 2 && (instruction.get(continuar + 1).equalsIgnoreCase("xml")
                    || instruction.get(continuar + 1).equals("json")));
        }
        return false;
    }

    /**
     * Revisa la sentencia update con cada sentencia que incluye ademas de la
     * sentencia where si hay
     *
     * @param size
     * @return
     */
    private boolean update(ArrayList<String> instruction, int size) {

        if (size < 6) {
            return false;
        }

        if (!(instruction.get(2).equalsIgnoreCase("set")) || isNumeric(instruction.get(1))
                || isNumeric(instruction.get(3)) || !instruction.get(4).equals("=")) {
            return false;
        }
        if (size == 6) {
            return true;
        }
        if (size < 10 || size > 11) {
            return false;
        }
        return whereStatement(instruction, 6, size, 10) != -1;  //metodo auxiliar el 6 equivale al punto de inicio, el size tamano
        // El 10 es el punto de requerimiento de finalizacion 
    }

    /**
     * El delete es similar al update revisa la sintaxis prestablecida y el
     * where statement si hay
     *
     * @param size
     * @return
     */
    private boolean delete(ArrayList<String> instruction, int size) {

        if (size < 3 || !(instruction.get(1).equalsIgnoreCase("from")) || isNumeric(instruction.get(2))) {
            return false;
        }
        if (size == 3) {
            return true;
        }
        if (size < 7 || size > 8) {
            return false;
        }                                                               //metodo auxiliar el 3 equivale al punto de inicio, el size tamano
        return whereStatement(instruction, 3, size, 7) != -1;   //  // El 7 es el punto de requeriemiento de finalizacion 
    }

    /**
     * El insert revisa que una la instruccion este bien formulada,
     * especialmente que el numero de columnas sea igual al numero de valores.
     * Ademas de su prestablecida sintaxis
     *
     * @param size
     * @return
     */
    private boolean insert(ArrayList<String> instruction, int size) {

        if (size < 10) {
            return false;
        }
        if (!(instruction.get(1).equalsIgnoreCase("into")) || isNumeric(instruction.get(2)) || !instruction.get(3).equals("(")) {
            return false;
        }
        int startValues = 0;
        boolean foundRBracket = false;
        int numberColumns = 0;
        boolean lastElement = false;    // si es , entonces es false;  si es columna entonces true
        for (int i = 4; i < size; i++) {

            String token = instruction.get(i);

            if (token.equals(")")) {
                foundRBracket = true;

                if (lastElement == false) {
                    return false;
                }
                startValues = i + 1;
                break;
            }
            if (!token.equals(",")) {  //es columna

                if (isNumeric(token) || lastElement == true) {
                    return false;
                }
                lastElement = true;
                numberColumns++;
            } else if (token.equals(",")) {

                if (lastElement == false) {
                    return false;
                }

                lastElement = false;
            } else {
                return false;
            }
        }
        if (!foundRBracket) {
            return false;
        }

        if (startValues + numberColumns > size || startValues == 0) {
            return false;
        }
        if (!instruction.get(startValues).equalsIgnoreCase("values") || !instruction.get(startValues + 1).equals("(")) {
            return false;
        }
        int numberValues = 0;
        lastElement = false;
        for (int i = startValues + 2; i < size; i++) {

            String token = instruction.get(i);

            if (token.equals(")")) {

                if (lastElement == false) {
                    return false;
                }
                if (numberValues == numberColumns && size == i + 1) {
                    return true;
                } else {
                    break;
                }
            }

            if (!token.equals(",")) {  //es valor

                if (lastElement == true) {
                    return false;
                }
                lastElement = true;
                numberValues++;

            } else if (token.equals(",")) {  //es ,

                if (lastElement == false) {
                    return false;
                }

                lastElement = false;
            } else {
                return false;
            }
        }
        return false;
    }

    /**
     * Muy importante metodo que revisa si un string es un numero o si incluye
     * un caracter no permitido, o una palabra reservada
     *
     * @param str
     * @return
     */
    private boolean isNumeric(String str) {

        if (str.isEmpty() || str.contains("+") || str.contains("-") || str.contains("*") || str.contains("?") || str.contains("!")
                || str.contains("(") || str.contains(")") || str.contains("%") || str.contains("&") || str.contains("@") || str.contains("#")
                || str.contains("$") || str.contains("^") || str.contains("|") || str.contains(";")) {
            return true;
        }
        if (str.equalsIgnoreCase("select") || str.equalsIgnoreCase("from") || str.equalsIgnoreCase("where")
                || str.equalsIgnoreCase("group") || str.equalsIgnoreCase("by") || str.equalsIgnoreCase("for")
                || str.equalsIgnoreCase("xml") || str.equalsIgnoreCase("json") || str.equalsIgnoreCase("join")
                || str.equalsIgnoreCase("update") || str.equalsIgnoreCase("set") || str.equalsIgnoreCase("delete")
                || str.equalsIgnoreCase("insert") || str.equalsIgnoreCase("into") || str.equalsIgnoreCase("values")
                || str.equalsIgnoreCase("create") || str.equalsIgnoreCase("database") || str.equalsIgnoreCase("table")
                || str.equalsIgnoreCase("databases") || str.equalsIgnoreCase("index") || str.equalsIgnoreCase("drop")
                || str.equalsIgnoreCase("start") || str.equalsIgnoreCase("get") || str.equalsIgnoreCase("status")
                || str.equalsIgnoreCase("stop") || str.equalsIgnoreCase("display") || str.equalsIgnoreCase("list")
                || str.equalsIgnoreCase("alter") || str.equalsIgnoreCase("foreign") || str.equalsIgnoreCase("key")
                || str.equalsIgnoreCase("on") || str.equalsIgnoreCase("references") || aggregate.contains(str)
                || comparators.contains(str)) {
            return true;
        }

        try {
            double d = Double.parseDouble(str);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

    /**
     * El join verifica que la lista de tablas con join esten bien escritas. Es
     * un auxilar en varios metodos principales
     *
     * @param startPoint
     * @param size
     * @return
     */
    private int join(ArrayList<String> instruction, int startPoint, int size) {
        if (startPoint + 1 > size) {
            return -1;
        }
        if (startPoint + 1 == size && !isNumeric(instruction.get(startPoint))) {
            return 0;
        }
        if (startPoint + 2 == size) {
            return -1;
        }
        if (startPoint + 2 < size) {
            String next = instruction.get(startPoint + 1);
            if (next.equalsIgnoreCase("where") || next.equalsIgnoreCase("group") || next.equalsIgnoreCase("for")) {
                return startPoint + 1;
            }
        }
        String currentToken;
        boolean lastElement = false;  // false si el ultimo fue un join, true si el ultimo fue una tabla

        for (int i = startPoint; i < size; i++) {

            currentToken = instruction.get(i);

            if (i + 1 == size) {

                if (!lastElement && !currentToken.equalsIgnoreCase("join")) {
                    return 0;
                } else {
                    return -1;
                }
            }
            if (i + 1 < size) {

                if (instruction.get(i + 1).equalsIgnoreCase("where") || instruction.get(i + 1).equalsIgnoreCase("group")
                        || instruction.get(i + 1).equalsIgnoreCase("for")) {

                    return i + 1;
                }
            }
            if (currentToken.equalsIgnoreCase("join")) {
                if (!lastElement) {
                    return -1;
                }
                lastElement = false;
            } else {
                if (isNumeric(currentToken) || lastElement == true) {
                    return -1;
                }
                lastElement = true;
            }
        }
        return -1;
    }

    /**
     * Revisa que la hilera de columnas esten bien formuladas con comas el punto
     * de inicio y final
     *
     * @param startPoint
     * @param size
     * @param endToken
     * @return
     */
    private int columns(ArrayList<String> instruction, int startPoint, int size, String endToken) {

        String startToken = instruction.get(startPoint);

        if (startToken.equals("*") && instruction.get(startPoint + 1).equalsIgnoreCase(endToken)) {
            return startPoint + 1;
        }
        String currentToken;
        boolean last = false;  // false si el ultimo fue una , 

        for (int i = startPoint; i < size; i++) {

            currentToken = instruction.get(i);
            if (!isNumeric(currentToken) && !currentToken.equals(",") && !currentToken.equals(endToken)) {
                if (last) {
                    return -1;
                }
                last = true;
                continue;
            }
            if (currentToken.equals(",")) {

                if (!last) {
                    return -1;
                }
                last = false;

                if (i + 3 < size) {
                    if (aggregate.contains(instruction.get(i + 1))) {
                        if (instruction.get(i + 2).equals("(") && !isNumeric(instruction.get(i + 3)) && instruction.get(i + 4).equals(")")
                                && instruction.get(i + 5).equalsIgnoreCase(endToken)) {
                            return i + 5;
                        }
                        return -1;
                    }
                } else {
                    System.out.println("aqui");
                    return -1;
                }

            } else if (currentToken.equals(endToken)) { //llega a from, sin pasar por los aggregate functions.

                if (!last) {
                    return -1;
                }
                return i;
            } else {
                return -1;
            }
        }
        System.out.println("aer");
        return -1;
    }

    /**
     * Metodo auxiliar para cuando se crea una tabla y se declaran las columnas
     *
     * @param startPoint
     * @param size
     * @param finishToken
     * @return
     */
    private int columnDefinition(ArrayList<String> instruction, int startPoint, int size, String finishToken) {

        int phase = 0;

        for (int i = startPoint; i < size; i++) {
            String token = instruction.get(i);
            if (phase == 0) {

                if (isNumeric(token) || token.equalsIgnoreCase("not") || token.equalsIgnoreCase("null")
                        || token.equalsIgnoreCase("integer") || token.equalsIgnoreCase("varchar") || token.equalsIgnoreCase("datetime")
                        || token.equalsIgnoreCase("decimal")) {
                    return -1;
                }
                phase++;
                continue;
            }
            if (phase == 1) {

                if (token.equalsIgnoreCase("integer") || token.equalsIgnoreCase("datetime")
                        || token.equalsIgnoreCase("varchar")) {
                    phase = 2;
                    continue;
                }
                if (token.equalsIgnoreCase("decimal") || token.equalsIgnoreCase("char")) {
                    continue;
                }
                String anterior = instruction.get(i - 1);
                if (token.equalsIgnoreCase("(") && (anterior.equalsIgnoreCase("decimal") || anterior.equalsIgnoreCase("char"))) {
                    continue;
                }
                if ((anterior.equals("(") || anterior.equals(",")) && isNumeric(token)) {
                    continue;
                }
                if (token.equals(")") && isNumeric(anterior)) {
                    phase = 2;
                    continue;
                }
                if (token.equals(",") && isNumeric(anterior)) {
                    continue;
                }
                return -1;
            }
            if (phase == 2) {

                if (token.equalsIgnoreCase("null")) {
                    phase = 3;
                    continue;
                }
                if (token.equalsIgnoreCase("not") && !instruction.get(i - 1).equalsIgnoreCase("not")) {
                } else {
                    return -1;
                }
            } else if (phase == 3) {

                if (token.equals(",") && instruction.get(i + 1).equals(finishToken)) {
                    return i;
                } else if (token.equals(",") && !instruction.get(i + 1).equals(finishToken)) {
                    phase = 0;
                } else {
                    return -1;
                }
            }
        }

        return -1;
    }

    /**
     * El where es un importante metodo auxiliar que revisa que la sentencia
     * where y los requerimientos de la instruccion principal sean cumplidos, en
     * extension y parametros.
     *
     * @param start
     * @param size
     * @param requirement
     * @return
     */
    private int whereStatement(ArrayList<String> instruction, int start, int size, int requirement) {

        if (!(instruction.get(start).equalsIgnoreCase("where")) || isNumeric(instruction.get(start + 1))) {
            return -1;
        }

        String position8 = instruction.get(start + 2);
        String position9 = instruction.get(start + 3);
        if ((comparators.contains(position8) || (position8.equals("is") && position9.equals("null"))) && size == requirement) {
            return requirement;
        }
        if (size != requirement + 1) {
            return -1;
        }

        String position10 = instruction.get(start + 4);
        if (position8.equals("is") && position9.equals("not") && position10.equals("null")) {
            return requirement + 1;
        } else {
            return -1;
        }
    }
}
