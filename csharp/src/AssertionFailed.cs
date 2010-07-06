namespace Perst
{
    using System;
	
    /// <summary> Exception raised by <code>Assert</code> class when assertion was failed.
    /// </summary>
    public class AssertionFailed:System.ApplicationException
    {
        internal AssertionFailed():base("Assertion failed")
        {
        }
		
        internal AssertionFailed(System.String description):base("Assertion '" + description + "' failed")
        {
        }
    }
}