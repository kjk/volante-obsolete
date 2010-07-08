using System;
using NachoDB.Impl;
using System.Runtime.Remoting.Contexts;
using System.Runtime.Remoting.Messaging;
using System.Runtime.Remoting.Activation;
  

namespace NachoDB
{
    /// <summary>
    /// Attribute providing transparent persistency for context bound objects.
    /// It should be used for classes derived from PeristentContext class.
    /// Objects of these classes automatically on demand load their 
    /// content from the database and also automatically detect object modification.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class TransparentPersistenceAttribute : ContextAttribute, IContributeObjectSink
    {
        public TransparentPersistenceAttribute() : base("PerstMOP")
        {
        }
   
        public override bool IsContextOK(Context ctx, IConstructionCallMessage ctor) 
        {
            return false;
        }
        public IMessageSink GetObjectSink(MarshalByRefObject target, IMessageSink next) 
        {
            return new PerstSink((PersistentContext)target, next);
        }
    }
}
