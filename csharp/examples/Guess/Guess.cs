using System;
using Volante;

public class Guess : Persistent
{
    public Guess yes;
    public Guess no;
    public string question;

    public Guess(Guess no, string question, Guess yes)
    {
        this.yes = yes;
        this.question = question;
        this.no = no;
    }

    internal Guess()
    {
    }

    internal static string input(string prompt)
    {
        while (true)
        {
            Console.Write(prompt);
            string line = Console.ReadLine().Trim();
            if (line.Length != 0)
            {
                return line;
            }
        }
    }

    internal static bool askQuestion(string question)
    {
        string answer = input(question);
        return answer.ToUpper().Equals("y".ToUpper()) || answer.ToUpper().Equals("yes".ToUpper());
    }

    internal static Guess whoIsIt(Guess parent)
    {
        string animal = input("What is it ? ");
        string difference = input("What is a difference from other ? ");
        return new Guess(parent, difference, new Guess(null, animal, null));
    }

    internal Guess dialog()
    {
        if (askQuestion("May be, " + question + " (y/n) ? "))
        {
            if (yes == null)
            {
                Console.WriteLine("It was very simple question for me...");
            }
            else
            {
                Guess clarify = yes.dialog();
                if (clarify != null)
                {
                    yes = clarify;
                    Store();
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
                    Store();
                }
            }
            else
            {
                Guess clarify = no.dialog();
                if (clarify != null)
                {
                    no = clarify;
                    Store();
                }
            }
        }
        return null;
    }

    static public void Main(string[] args)
    {
        IDatabase db = DatabaseFactory.CreateDatabase();

        Rc4File dbFile = new Rc4File("guess.db", "GUESS");
        db.Open(dbFile, 4 * 1024 * 1024);
        Guess root = (Guess)db.Root;

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
            db.Commit();
        }

        Console.WriteLine("End of the game");
        db.Close();
    }
}