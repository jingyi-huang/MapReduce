import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

/**
 * Created by kaiyanglyu on 2/23/17.
 */
public class writeText {
    public static void main (String[] arge) throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter writer = new PrintWriter("RandomTextFile.txt", "UTF-8");
        wordsBag wb = new wordsBag();
        writer.println(wb.generateSentence(10000));
        writer.close();
    }
}
