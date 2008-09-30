// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details

using System;
using System.Reflection;
using System.Runtime.Remoting.Proxies;
using System.Runtime.Remoting.Messaging;

namespace Wv
{
	//marked internal because this is really an implementation detail and needs to be replaced
	internal class DProxy : RealProxy
	{
		protected BusObject busObject;

		public DProxy (BusObject busObject, Type type) : base(type)
		{
			this.busObject = busObject;
		}

		static MethodInfo mi_GetHashCode = typeof (object).GetMethod ("GetHashCode");
		static MethodInfo mi_Equals = typeof (object).GetMethod ("Equals", BindingFlags.Instance);
		static MethodInfo mi_ToString = typeof (object).GetMethod ("ToString");
		static MethodInfo mi_GetLifetimeService = typeof (MarshalByRefObject).GetMethod ("GetLifetimeService");

		object GetDefaultReturn (MethodBase mi, object[] inArgs)
		{
			if (mi == mi_GetHashCode)
			        return busObject.Path.GetHashCode ();
			if (mi == mi_Equals)
				return busObject.Path == ((BusObject)((MarshalByRefObject)inArgs[0]).GetLifetimeService ()).Path;
			if (mi == mi_ToString)
				return busObject.Path;
			if (mi == mi_GetLifetimeService)
				return busObject;

			return null;
		}

		public override IMessage Invoke (IMessage message)
		{
			IMethodCallMessage callMessage = (IMethodCallMessage) message;

			object defaultRetVal = GetDefaultReturn (callMessage.MethodBase, callMessage.InArgs);
			if (defaultRetVal != null) {
				MethodReturnMessageWrapper defaultReturnMessage = new MethodReturnMessageWrapper ((IMethodReturnMessage) message);
				defaultReturnMessage.ReturnValue = defaultRetVal;

				return defaultReturnMessage;
			}

			object[] outArgs;
			object retVal;
			Exception exception;
			busObject.Invoke (callMessage.MethodBase, callMessage.MethodName, callMessage.InArgs, out outArgs, out retVal, out exception);

		        if (exception != null)
			    return new ReturnMessage(exception, callMessage);
		        else 
			    return new ReturnMessage(retVal, outArgs, 
				     outArgs.Length, null, callMessage);
		}

		/*
		public override ObjRef CreateObjRef (Type ServerType)
		{
			throw new System.NotImplementedException ();
		}
		*/

		~DProxy ()
		{
			//FIXME: remove handlers/match rules here
			if (Protocol.Verbose)
				Console.Error.WriteLine ("Warning: Finalization of " + busObject.Path + " not yet supported");
		}
	}
}
