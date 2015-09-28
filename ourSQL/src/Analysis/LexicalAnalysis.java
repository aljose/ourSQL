/*
 * Lexical DatabaseRuntimeProcessor.Analysis
 * Analiza todo un conjunto de instruciones  
 * 
 */
package Analysis;

import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 *
 * @author Nicolas Jimenez
 */
public class LexicalAnalysis {

    /**
     *  Instruccion que tokeniza las instrucciones por espacios y separa instruccion por instruccion por ";" 
     * @param instruction 
     * @return  
     */
    public ArrayList<String> tokenize(String instruction) {
        
        ArrayList<String> instruccions = new ArrayList<String>();
        StringTokenizer tokens = new StringTokenizer(instruction);

        while (tokens.hasMoreElements()) {

            String elementoActual = tokens.nextToken();

                instruccions.add( elementoActual);
        }
        return instruccions;
    }


    
}
