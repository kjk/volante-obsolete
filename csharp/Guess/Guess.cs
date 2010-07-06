using System;
using Perst;

public class Guess:Persistent
{
    public Guess  yes;
    public Guess  no;
    public String question;
	
    public Guess(Guess no, System.String question, Guess yes)
    {
        this.yes = yes;
        this.question = question;
        this.no = no;
    }
	
    internal Guess()
    {
    }
	
    internal static System.String input(System.String prompt)
    {
        while (true)
        {
            Console.Write(prompt);
            String line = Console.ReadLine().Trim();
            if (line.Length != 0) 
            { 
                return line;
            }
        }
    }
	
    internal static bool askQuestion(System.String question)
    {
        System.String answer = input(question);
        return answer.ToUpper().Equals("y".ToUpper()) || answer.ToUpper().Equals("yes".ToUpper());
    }
	
    internal static Guess whoIsIt(Guess parent)
    {
        System.String animal = input("What is it ? ");
        System.String difference = input("What is a difference from other ? ");
        return new Guess(parent, difference, new Guess(null, animal, null));
    }
	
    internal Guess dialog()
    {
        if (askQuestion("May be, " + question + " (y/n) ? "))
        {
            if (yes == null)
            {
                System.Console.Out.WriteLine("It was very simple question for me...");
            }
            else
            {
                Guess clarify = yes.dialog();
                if (clarify != null)
                {
                    yes = clarify;
                    store();
                }
            }
        }
        else
        {
            if (no == null)
            {
                if (yes == null)
                {
                    return whoIsIt(this);
                }
                else
                {
                    no = whoIsIt(null);
                    store();
                }
            }
            else
            {
                Guess clarify = no.dialog();
                if (clarify != null)
                {
                    no = clarify;
                    store();
                }
            }
        }
        return null;
    }
	
    static public void  Main(System.String[] args)
    {
        Storage db = StorageFactory.Instance.createStorage();
		
        db.open("guess.dbs");
        Guess root = (Guess) db.Root;
		
        while (askQuestion("Think of an animal. Ready (y/n) ? "))
        {
            if (root == null)
            {
                root = whoIsIt(null);
                db.Root = root;
            }
            else
            {
                root.dialog();
            }
            db.commit();
        }
		
        System.Console.Out.WriteLine("End of the game");
        db.close();
    }
}