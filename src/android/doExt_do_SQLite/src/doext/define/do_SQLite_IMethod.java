package doext.define;

import core.helper.jsonparse.DoJsonNode;
import core.interfaces.DoIScriptEngine;
import core.object.DoInvokeResult;

/**
 * 声明自定义扩展组件方法
 */
public interface do_SQLite_IMethod {
	void close(DoJsonNode _dictParas,DoIScriptEngine _scriptEngine, DoInvokeResult _invokeResult) throws Exception ;
	void executeSync(DoJsonNode _dictParas,DoIScriptEngine _scriptEngine, DoInvokeResult _invokeResult) throws Exception ;
	void execute(DoJsonNode _dictParas,DoIScriptEngine _scriptEngine, String _callbackFuncName) throws Exception ;
	void open(DoJsonNode _dictParas,DoIScriptEngine _scriptEngine, DoInvokeResult _invokeResult) throws Exception ;
	void query(DoJsonNode _dictParas,DoIScriptEngine _scriptEngine, String _callbackFuncName) throws Exception ;
}