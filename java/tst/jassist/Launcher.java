import javassist.*;
import org.garret.perst.jassist.PerstTranslator;

public class Launcher { 
    public static void main(String[] args) throws Throwable { 
        if (args.length != 1) {
            System.err.println("Usage: java Laucher CLASS-NAME");
        } else { 
            Translator trans = new PerstTranslator();
            ClassPool pool = ClassPool.getDefault(trans);
            Loader cl = new Loader(pool);
            Thread.currentThread().setContextClassLoader(cl);
            cl.run(args[0], args);
        }
    }
}