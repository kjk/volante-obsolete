#if WITH_XML
namespace Volante
{
    using System;

    /// <summary> Exception thrown during import of data from XML file in database
    /// </summary>
    public class XmlImportException : ApplicationException
    {
        public virtual System.String MessageText
        {
            get
            {
                return message;
            }

        }
        public virtual int Line
        {
            get
            {
                return line;
            }

        }
        public virtual int Column
        {
            get
            {
                return column;
            }

        }
        public XmlImportException(int line, int column, String message)
            : base("In line " + line + " column " + column + ": " + message)
        {
            this.line = line;
            this.column = column;
            this.message = message;
        }

        private int line;
        private int column;
        private String message;
    }
}
#endif
