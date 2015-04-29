package doext.implement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteStatement;
import core.DoServiceContainer;
import core.helper.DoIOHelper;
import core.helper.jsonparse.DoJsonNode;
import core.helper.jsonparse.DoJsonValue;
import core.interfaces.DoIScriptEngine;
import core.object.DoInvokeResult;
import doext.define.do_SQLite_IMethod;
import doext.define.do_SQLite_MAbstract;

/**
 * 自定义扩展MM组件Model实现，继承do_SQLite_MAbstract抽象类，并实现do_SQLite_IMethod接口方法；
 * #如何调用组件自定义事件？可以通过如下方法触发事件：
 * this.model.getEventCenter().fireEvent(_messageName, jsonResult);
 * 参数解释：@_messageName字符串事件名称，@jsonResult传递事件参数对象；
 * 获取DoInvokeResult对象方式new DoInvokeResult(this.getUniqueKey());
 */
public class do_SQLite_Model extends do_SQLite_MAbstract implements do_SQLite_IMethod {
	
	private SQLiteDatabase database;
	
	public do_SQLite_Model() throws Exception {
		super();
	}
	
	@Override
	public boolean onPropertiesChanging(Map<String, String> _changedValues)
			throws Exception {
		if(_changedValues.containsKey("path")){
			String path  = _changedValues.get("path");
			if ("".equals(path) || null == path) {
				return false;
			}
		}
		if(_changedValues.containsKey("sql")){
			String sql  = _changedValues.get("sql");
			if ("".equals(sql) || null == sql) {
				return false;
			}
		}
		return super.onPropertiesChanging(_changedValues);
	}

	/**
	 * 同步方法，JS脚本调用该组件对象方法时会被调用，可以根据_methodName调用相应的接口实现方法；
	 * @_methodName 方法名称
	 * @_dictParas 参数（K,V）
	 * @_scriptEngine 当前Page JS上下文环境对象
	 * @_invokeResult 用于返回方法结果对象
	 */
	@Override
	public boolean invokeSyncMethod(String _methodName, DoJsonNode _dictParas,
			DoIScriptEngine _scriptEngine, DoInvokeResult _invokeResult)
			throws Exception {
		if ("open".equals(_methodName)) {
			open(_dictParas, _scriptEngine, _invokeResult);
			return true;
		}
		if ("close".equals(_methodName)) {
			close(_dictParas, _scriptEngine, _invokeResult);
			return true;
		}
		if ("executeSync".equals(_methodName)) {
			executeSync(_dictParas, _scriptEngine, _invokeResult);
			return true;
		}
		return super.invokeSyncMethod(_methodName, _dictParas, _scriptEngine, _invokeResult);
	}
	
	/**
	 * 异步方法（通常都处理些耗时操作，避免UI线程阻塞），JS脚本调用该组件对象方法时会被调用，
	 * 可以根据_methodName调用相应的接口实现方法；
	 * @_methodName 方法名称
	 * @_dictParas 参数（K,V）
	 * @_scriptEngine 当前page JS上下文环境
	 * @_callbackFuncName 回调函数名
	 * #如何执行异步方法回调？可以通过如下方法：
	 * _scriptEngine.callback(_callbackFuncName, _invokeResult);
	 * 参数解释：@_callbackFuncName回调函数名，@_invokeResult传递回调函数参数对象；
	 * 获取DoInvokeResult对象方式new DoInvokeResult(this.getUniqueKey());
	 */
	@Override
	public boolean invokeAsyncMethod(String _methodName, DoJsonNode _dictParas,
			DoIScriptEngine _scriptEngine, String _callbackFuncName) throws Exception{
		if ("execute".equals(_methodName)) {
			execute(_dictParas, _scriptEngine, _callbackFuncName);
			return true;
		}
		if ("query".equals(_methodName)) {
			query(_dictParas, _scriptEngine, _callbackFuncName);
			return true;
		}
		return super.invokeAsyncMethod(_methodName, _dictParas, _scriptEngine, _callbackFuncName);
	}
	
	/**
	 * 打开数据库；
	 * @_dictParas 参数（K,V），可以通过此对象提供相关方法来获取参数值（Key：为参数名称）；
	 * @_scriptEngine 当前Page JS上下文环境对象
	 * @_invokeResult 用于返回方法结果对象
	 */
	@Override
	public void open(DoJsonNode _dictParas, DoIScriptEngine _scriptEngine,
			DoInvokeResult _invokeResult) throws Exception {
		String path = _dictParas.getOneText("path", "");
		if ("".equals(path) || null == path) {
			DoServiceContainer.getLogEngine().writeInfo("Do_SQLite","打开数据库失败：path" + path);
		}else{
			if (":memory:".equalsIgnoreCase(path)) {
				database = SQLiteDatabase.create(null);
			} else {
				String dbPath = _scriptEngine.getCurrentApp().getDataFS().getFileFullPathByName(path);
				createDBFile(dbPath);
				database = SQLiteDatabase.openOrCreateDatabase(dbPath, null);
			}
		}
	}
	
	/**
	 * 创建数据库DB文件
	 * @param dbPath
	 * @throws IOException
	 */
	private void createDBFile(String dbPath) throws IOException {
		String _directory = dbPath.substring(0, dbPath.lastIndexOf("/"));
		DoIOHelper.createDirectory(_directory);
		DoIOHelper.createFile(dbPath);
	}
	
	/**
	 * 关闭数据库；
	 * @_dictParas 参数（K,V），可以通过此对象提供相关方法来获取参数值（Key：为参数名称）；
	 * @_scriptEngine 当前Page JS上下文环境对象
	 * @_invokeResult 用于返回方法结果对象
	 */
	@Override
	public void close(DoJsonNode _dictParas, DoIScriptEngine _scriptEngine,
			DoInvokeResult _invokeResult) throws Exception {
		if(null != database){
			database.close();
		}
	}
	
	/**
	 * 执行SQL语句；
	 * @throws Exception 
	 * @_dictParas 参数（K,V），可以通过此对象提供相关方法来获取参数值（Key：为参数名称）；
	 * @_scriptEngine 当前Page JS上下文环境对象
	 * @_callbackFuncName 回调函数名
	 */
	@Override
	public void executeSync(DoJsonNode _dictParas, DoIScriptEngine _scriptEngine,
			DoInvokeResult _invokeResult) throws Exception {
		String _sql = _dictParas.getOneText("sql","").trim();
		DoInvokeResult invokeResult = new DoInvokeResult(getUniqueKey());
		try {
			execSQL(_sql);
			invokeResult.setResultBoolean(true);
		} catch (Exception _err) {
			invokeResult.setResultBoolean(false);
			DoServiceContainer.getLogEngine().writeError("SQLite", _err);
		}
	}
	
	/**
	 * 执行SQL语句；
	 * @throws Exception 
	 * @_dictParas 参数（K,V），可以通过此对象提供相关方法来获取参数值（Key：为参数名称）；
	 * @_scriptEngine 当前Page JS上下文环境对象
	 * @_callbackFuncName 回调函数名
	 */
	@Override
	public void execute(DoJsonNode _dictParas, DoIScriptEngine _scriptEngine,String _callbackFuncName) throws Exception {
		String _sql = _dictParas.getOneText("sql","").trim();
		DoInvokeResult invokeResult = new DoInvokeResult(getUniqueKey());
		try {
			execSQL(_sql);
			invokeResult.setResultBoolean(true);
		} catch (Exception _err) {
			invokeResult.setResultBoolean(false);
			DoServiceContainer.getLogEngine().writeError("SQLite", _err);
		} finally {
			_scriptEngine.callback(_callbackFuncName, invokeResult);
		}
	}
	
	private void execSQL(String sql){
		if("".equals(sql) || null == sql){
			throw new RuntimeException("执行SQL失败，sql：" + sql);
		}
		String _sql_prefix = "";
		if(sql.length() >= 6){
			_sql_prefix = sql.substring(0, 6);
		}
		if (("INSERT").equalsIgnoreCase(_sql_prefix)) {
			executeInsert(sql);
		} else {
			database.execSQL(sql);
		}
	}
	
	private void executeInsert(String _sql){
		String sqls[] = _sql.split(";");
		database.beginTransaction();
		try{
			for (String sql : sqls) {
				SQLiteStatement statement = database.compileStatement(sql);
				statement.executeInsert();
			}
			database.setTransactionSuccessful();
		}catch(SQLException e){
			e.printStackTrace();
			throw new RuntimeException(e);
		}finally{
			database.endTransaction();
		}
	}

	private List<DoJsonValue> getQueryResult(Cursor cursor, String sql) throws Exception{
		int count = cursor.getCount();
		List<DoJsonValue> rows = new ArrayList<DoJsonValue>();
		if (cursor.moveToFirst()) {
			String[] columnNames = cursor.getColumnNames();
			for (int i = 0; i < count; i++) {
				cursor.moveToPosition(i);
				DoJsonNode colNode = new DoJsonNode();
				DoJsonValue colValue = new DoJsonValue();
				for (int j = 0; j < columnNames.length; j++) {
					String columnName = columnNames[j];
					String value = cursor.getString(cursor.getColumnIndex(columnName));
					colNode.setOneText(columnName, value);
					colValue.setNode(colNode);
				}
				rows.add(colValue);
			}
		}
		return rows;
	}

	/**
	 * 执行SQL查询语句；
	 * @throws Exception 
	 * @_dictParas 参数（K,V），可以通过此对象提供相关方法来获取参数值（Key：为参数名称）；
	 * @_scriptEngine 当前Page JS上下文环境对象
	 * @_callbackFuncName 回调函数名
	 */
	@Override
	public void query(DoJsonNode _dictParas, DoIScriptEngine _scriptEngine,String _callbackFuncName) throws Exception {
		String _sql = _dictParas.getOneText("sql","").trim();
		DoInvokeResult invokeResult = new DoInvokeResult(getUniqueKey());
		Cursor cursor = null;
		try {
			if("".equals(_sql) || null == _sql){
				throw new RuntimeException("SQLite查询失败，sql：" + _sql);
			}
			cursor = database.rawQuery(_sql, new String[] {});
			invokeResult.setResultArray(getQueryResult(cursor,_sql));
			_scriptEngine.callback(_callbackFuncName, invokeResult);
		} catch (Exception _err) {
			DoServiceContainer.getLogEngine().writeError("SQLite", _err);
		} finally {
			if(cursor!=null && !cursor.isClosed()){
				cursor.close();
			}
		}
	}
}