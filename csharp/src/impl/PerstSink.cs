using System;
using System.Reflection;
using System.Runtime.Remoting.Messaging;

namespace Perst.Impl
{
    public class PerstSink : IMessageSink 
    {
        internal PerstSink(PersistentContext target, IMessageSink next) 
        {
            this.next = next;
            this.target = target;
        }

        public IMessageSink NextSink 
        {
            get 
            { 
                return next;
            }
        }

        public IMessage SyncProcessMessage(IMessage call) 
        {
            IMethodMessage invocation = (IMethodMessage)call;
            if (invocation.TypeName != "Perst.PersistentContext") 
            { 
                target.Load();
                if (invocation.MethodName == "FieldSetter") 
                {
                    target.Modify();
                }
            }
            return NextSink.SyncProcessMessage(call);
        }

        public IMessageCtrl AsyncProcessMessage(IMessage call, IMessageSink destination) 
        {
            return NextSink.AsyncProcessMessage(call, destination);
        }

        private IMessageSink      next;
        private PersistentContext target;
    }
}
