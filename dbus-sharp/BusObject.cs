// Copyright 2006 Alp Toker <alp@atoker.com>
// This software is made available under the MIT License
// See COPYING for details

using System;
using System.Reflection;
using System.Reflection.Emit;
using System.Collections.Generic;

namespace Wv
{
	public class BusObject
	{
		protected Connection conn;
		string bus_name;
		string object_path;

		//protected BusObject ()
		public BusObject ()
		{
		}

		public BusObject (Connection conn, string bus_name, string object_path)
		{
			this.conn = conn;
			this.bus_name = bus_name;
			this.object_path = object_path;
		}

		public Connection Connection
		{
			get {
				return conn;
			}
		}

		public string BusName
		{
			get {
				return bus_name;
			}
		}

		public string Path
		{
			get {
				return object_path;
			}
		}

		public void ToggleSignal (string iface, string member, Delegate dlg, bool adding)
		{
			MatchRule rule = new MatchRule ();
			rule.MessageType = MessageType.Signal;
			rule.Interface = iface;
			rule.Member = member;
			rule.Path = object_path;

			if (adding) {
				if (conn.Handlers.ContainsKey (rule))
					conn.Handlers[rule] = Delegate.Combine (conn.Handlers[rule], dlg);
				else {
					conn.Handlers[rule] = dlg;
					conn.AddMatch (rule.ToString ());
				}
			} else {
				conn.Handlers[rule] = Delegate.Remove (conn.Handlers[rule], dlg);
				if (conn.Handlers[rule] == null) {
					conn.RemoveMatch (rule.ToString ());
					conn.Handlers.Remove (rule);
				}
			}
		}

		public void SendSignal (string iface, string member, string inSigStr, MessageWriter writer, Type retType, out Exception exception)
		{
			exception = null;

			//TODO: don't ignore retVal, exception etc.

			Signal signal = new Signal (object_path, iface, member);
			signal.msg.signature = inSigStr;

			Message signalMsg = signal.msg;
			signalMsg.Body = writer.ToArray ();

			conn.Send (signalMsg);
		}

		public object SendMethodCall (string iface, string member, string inSigStr, MessageWriter writer, Type retType, out Exception exception)
		{
			exception = null;

			//TODO: don't ignore retVal, exception etc.

			MethodCall method_call = new MethodCall (object_path, iface, member, bus_name, inSigStr);

			Message callMsg = method_call.msg;
			callMsg.Body = writer.ToArray ();

			Type[] outTypes = new Type[1];
			outTypes[0] = retType;

			//we default to always requiring replies for now, even though unnecessary
			//this is to make sure errors are handled synchronously
			//TODO: don't hard code this
			bool needsReply = true;

			callMsg.ReplyExpected = needsReply;
			callMsg.signature = inSigStr;

			if (!needsReply) {
				conn.Send (callMsg);
				return null;
			}

			Message retMsg = conn.SendWithReplyAndBlock (callMsg);

			object retVal = null;

			//handle the reply message
			switch (retMsg.type) 
		        {
			case MessageType.MethodReturn:
				object[] retVals = MessageHelper.GetDynamicValues (retMsg, outTypes);
				if (retVals.Length != 0)
					retVal = retVals[retVals.Length - 1];
				break;
			case MessageType.Error:
				//TODO: typed exceptions
				Error error = new Error (retMsg);
				string errMsg = String.Empty;
				if (retMsg.Signature.Value.StartsWith ("s")) {
				        errMsg = retMsg.iter().pop();
				}
				exception = new Exception (error.ErrorName + ": " + errMsg);
				break;
			default:
				throw new Exception ("Got unexpected message of type " + retMsg.type + " while waiting for a MethodReturn or Error");
			}

			return retVal;
		}

		public void Invoke (MethodBase methodBase, string methodName, object[] inArgs, out object[] outArgs, out object retVal, out Exception exception)
		{
			outArgs = new object[0];
			retVal = null;
			exception = null;

			MethodInfo mi = methodBase as MethodInfo;

			if (mi != null && mi.IsSpecialName && (methodName.StartsWith ("add_") || methodName.StartsWith ("remove_"))) {
				string[] parts = methodName.Split (new char[]{'_'}, 2);
				string ename = parts[1];
				Delegate dlg = (Delegate)inArgs[0];

				ToggleSignal (Mapper.GetInterfaceName (mi), ename, dlg, parts[0] == "add");

				return;
			}

			Type[] inTypes = Mapper.GetTypes (ArgDirection.In, mi.GetParameters ());
			Signature inSig = Signature.GetSig (inTypes);

			MethodCall method_call;
			Message callMsg;

			//build the outbound method call message
			{
				//this bit is error-prone (no null checking) and will need rewriting when DProxy is replaced
				string iface = null;
				if (mi != null)
					iface = Mapper.GetInterfaceName (mi);

				//map property accessors
				//TODO: this needs to be done properly, not with simple String.Replace
				//note that IsSpecialName is also for event accessors, but we already handled those and returned
				if (mi != null && mi.IsSpecialName) {
					methodName = methodName.Replace ("get_", "Get");
					methodName = methodName.Replace ("set_", "Set");
				}

				method_call = new MethodCall (object_path, iface, methodName, bus_name, inSig.ToString());

				callMsg = method_call.msg;

				if (inArgs != null && inArgs.Length != 0) {
					MessageWriter writer = new MessageWriter (Connection.NativeEndianness);
					writer.connection = conn;

					for (int i = 0 ; i != inTypes.Length ; i++)
						writer.Write (inTypes[i], inArgs[i]);

					callMsg.Body = writer.ToArray ();
				}
			}

			Type[] outTypes = new Type[1];
			outTypes[0] = mi.ReturnType;

			//we default to always requiring replies for now, even though unnecessary
			//this is to make sure errors are handled synchronously
			//TODO: don't hard code this
			bool needsReply = true;

			callMsg.ReplyExpected = needsReply;
			callMsg.signature = inSig.ToString();

			if (!needsReply) {
				conn.Send (callMsg);
				return;
			}

			Message retMsg = conn.SendWithReplyAndBlock (callMsg);

			//handle the reply message
			switch (retMsg.type) {
			case MessageType.MethodReturn:
				object[] retVals = MessageHelper.GetDynamicValues (retMsg, outTypes);
				if (retVals.Length != 0)
					retVal = retVals[retVals.Length - 1];
				break;
			case MessageType.Error:
				//TODO: typed exceptions
				Error error = new Error (retMsg);
				string errMsg = String.Empty;
				if (retMsg.Signature.Value.StartsWith ("s")) {
				        errMsg = retMsg.iter().pop();
				}
				exception = new Exception (error.ErrorName + ": " + errMsg);
				break;
			default:
				throw new Exception ("Got unexpected message of type " + retMsg.type + " while waiting for a MethodReturn or Error");
			}

			return;
		}

		public static object GetObject (Connection conn, string bus_name, string object_path, Type declType)
		{
			Type proxyType = TypeImplementer.GetImplementation (declType);

			BusObject inst = (BusObject)Activator.CreateInstance (proxyType);
			inst.conn = conn;
			inst.bus_name = bus_name;
			inst.object_path = object_path;

			return inst;
		}

		public Delegate GetHookupDelegate (EventInfo ei)
		{
			DynamicMethod hookupMethod = TypeImplementer.GetHookupMethod (ei);
			Delegate d = hookupMethod.CreateDelegate (ei.EventHandlerType, this);
			return d;
		}
	}
}
