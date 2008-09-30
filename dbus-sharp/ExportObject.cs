// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details

using System;
using System.Reflection;
using System.Reflection.Emit;

using org.freedesktop.DBus;

namespace Wv
{
	//TODO: perhaps ExportObject should not derive from BusObject
	internal class ExportObject : BusObject //, Peer
	{
		public readonly object obj;

		public ExportObject (Connection conn, string object_path, object obj) : base (conn, null, object_path)
		{
			this.obj = obj;
		}

		//maybe add checks to make sure this is not called more than once
		//it's a bit silly as a property
		public bool Registered
		{
			set {
				Type type = obj.GetType ();

				foreach (MemberInfo mi in Mapper.GetPublicMembers (type)) {
					EventInfo ei = mi as EventInfo;

					if (ei == null)
						continue;

					Delegate dlg = GetHookupDelegate (ei);

					if (value)
						ei.AddEventHandler (obj, dlg);
					else
						ei.RemoveEventHandler (obj, dlg);
				}
			}
		}

		public void HandleMethodCall (MethodCall method_call)
		{
			Type type = obj.GetType ();
			//object retObj = type.InvokeMember (msg.Member, BindingFlags.InvokeMethod, null, obj, MessageHelper.GetDynamicValues (msg));

			//TODO: there is no member name mapping for properties etc. yet
			MethodInfo mi = Mapper.GetMethod (type, method_call);

			if (mi == null) {
				conn.MaybeSendUnknownMethodError (method_call);
				return;
			}

			object retObj = null;
			object[] parmValues = MessageHelper.GetDynamicValues (method_call.msg, mi.GetParameters ());

			try {
				retObj = mi.Invoke (obj, parmValues);
			} catch (TargetInvocationException e) {
				if (!method_call.msg.ReplyExpected)
					return;

				Exception ie = e.InnerException;
				//TODO: complete exception sending support

				Error error = new Error (Mapper.GetInterfaceName (ie.GetType ()), method_call.msg.serial);
				error.msg.signature = DType.String.ToString();

				MessageWriter writer = new MessageWriter (Connection.NativeEndianness);
				writer.connection = conn;
				writer.Write(ie.Message);
				error.msg.Body = writer.ToArray ();

				//TODO: we should be more strict here, but this fallback was added as a quick fix for p2p
				if (method_call.Sender != null)
					error.msg.dest = method_call.Sender;

				conn.Send (error.msg);
				return;
			}

			if (method_call.msg.ReplyExpected) {
				Message reply = MessageHelper.ConstructDynamicReply (method_call, mi, retObj, parmValues);
				conn.Send (reply);
			}
		}
	}
}
