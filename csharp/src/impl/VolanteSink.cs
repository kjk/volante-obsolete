using System;
using System.Reflection;
using System.Runtime.Remoting.Messaging;

namespace Volante.Impl
{
    public class VolanteSink : IMessageSink
    {
        internal VolanteSink(PersistentContext target, IMessageSink next)
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
            if (invocation.TypeName != "Volante.PersistentContext")
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

        private IMessageSink next;
        private PersistentContext target;
    }
}
