namespace Perst
{
    using System;
	
    /// <summary>Class for checking program invariants. Analog of C <code>assert()</code>
    /// macro. The Java compiler doesn't provide information about compiled
    /// file and line number, so the place of assertion failure can be located only
    /// by analyzing the stack trace of the thrown AssertionFailed exception.
    /// *
    /// </summary>
    /// <seealso cref="Perst.AssertionFailed"/>
    public class Assert
    {
        /// <summary>Check specified condition and raise <code>AssertionFailed</code>
        /// exception if it is not true.
        /// 
        /// </summary>
        /// <param name="cond">result of checked condition 
        /// 
        /// </param>
        public static void  That(bool cond)
        {
            if (!cond)
            {
                throw new AssertionFailed();
            }
        }
		
        /// <summary>Check specified condition and raise <code>AssertionFailed</code>
        /// exception if it is not true. 
        /// 
        /// </summary>
        /// <param name="description">string describing checked condition
        /// </param>
        /// <param name="cond">result of checked condition 
        /// 
        /// </param>
        public static void  That(System.String description, bool cond)
        {
            if (!cond)
            {
                throw new AssertionFailed(description);
            }
        }
		
        /// <summary> Throw assertion failed exception.
        /// </summary>
        public static void  Failed()
        {
            throw new AssertionFailed();
        }
		
        /// <summary> Throw assertion failed exception with given description.
        /// </summary>
        public static void  Failed(System.String description)
        {
            throw new AssertionFailed(description);
        }
    }
}