package doext.implement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteStatement;
import core.DoServiceContainer;
import core.helper.jsonparse.DoJsonNode;
import core.helper.jsonparse.DoJsonValue;
import core.interfaces.DoIListData;
import core.interfaces.DoIScriptEngine;
import core.interfaces.datamodel.DoIDataSource;
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
public class do_SQLite_Model extends do_SQLite_MAbstract implements do_SQLite_IMethod, DoIDataSource, DoIListData {
	
	private SQLiteDatabase database;
	private List<DoJsonValue> modelData;
	
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
		String path = _dictParas.getOneText("path", getPropertyValue("path"));
		if ("".equals(path) || null == path) {
			DoServiceContainer.getLogEngine().writeInfo("Do_SQLite","打开数据库失败：path" + path);
		}else{
			if (":memory:".equalsIgnoreCase(path)) {
				database = SQLiteDatabase.create(null);
			} else {
				String dbPath = _scriptEngine.getCurrentApp().getDataFS().getFileFullPathByName(path);
				database = SQLiteDatabase.openOrCreateDatabase(dbPath, null);
			}
		}
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
	public void execute(DoJsonNode _dictParas, DoIScriptEngine _scriptEngine,String _callbackFuncName) throws Exception {
		String _sql = _dictParas.getOneText("sql", getPropertyValue("sql")).trim();
		DoInvokeResult invokeResult = new DoInvokeResult(getUniqueKey());
		try {
			if("".equals(_sql) || null == _sql){
				throw new RuntimeException("执行SQL失败，sql：" + _sql);
			}
			String _sql_prefix = "";
			if(_sql.length() >= 6){
				_sql_prefix = _sql.substring(0, 6);
			}
			if (("INSERT").equalsIgnoreCase(_sql_prefix)) {
				executeInsert(_sql);
			} else if (("DELETE").equalsIgnoreCase(_sql_prefix)) {
				 executeDelete(_sql);
			} else if (("UPDATE").equalsIgnoreCase(_sql_prefix)) {
				 executeUpdate(_sql);
			} else {
				database.execSQL(_sql);
			}
			invokeResult.setResultBoolean(true);
		} catch (Exception _err) {
			invokeResult.setResultBoolean(false);
			DoServiceContainer.getLogEngine().writeError("SQLite", _err);
		} finally {
			_scriptEngine.callback(_callbackFuncName, invokeResult);
		}
	}
	
	private int executeInsert(String _sql){
		int rows = 0;
		String sqls[] = _sql.split(";");
		database.beginTransaction();
		try{
			for (String sql : sqls) {
				SQLiteStatement statement = database.compileStatement(sql);
				statement.executeInsert();
				rows++;
			}
			database.setTransactionSuccessful();
		}catch(SQLException e){
			e.printStackTrace();
			throw new RuntimeException(e);
		}finally{
			database.endTransaction();
		}
		return rows;
	}

	private int executeUpdate(String _sql) {
		String table = _sql.substring(6, _sql.indexOf("SET")).trim();
		String setClause = _sql.substring(_sql.indexOf("SET") + 3, _sql.indexOf("WHERE")).trim();
		String whereClause = _sql.substring(_sql.indexOf("WHERE") + 5).trim();
		ContentValues values = new ContentValues();
		for (String clause : setClause.split(",")) {
			String key = clause.split("=")[0].trim();
			String value = clause.split("=")[1].trim().replaceAll("'", "");
			values.put(key, value);
		}
		return database.update(table, values, whereClause, null);
	}

	private int executeDelete(String _sql) {
		String table = _sql.substring(_sql.indexOf("FROM") + 4, _sql.indexOf("WHERE")).trim();
		String whereClause = _sql.substring(_sql.indexOf("WHERE") + 5).trim();
		return database.delete(table, whereClause, null);
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
		String _sql = _dictParas.getOneText("sql", getPropertyValue("sql")).trim();
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
	
	@Override
	public void getJsonData(final DoGetJsonCallBack _callback) throws Exception {
		new Thread(new Runnable() {
			@Override
			public void run() {
				Cursor cursor = null;
				try {
					String _sql = getPropertyValue("sql");
					if("".equals(_sql) || null == _sql){
						throw new RuntimeException("SQLite查询失败，sql：" + _sql);
					}
					cursor = database.rawQuery(_sql, new String[] {});
					modelData = getQueryResult(cursor,_sql);
					_callback.doGetJsonCallBack(do_SQLite_Model.this);
				} catch (Exception e) {
					DoServiceContainer.getLogEngine().writeError("SQLite", e);
				}finally {
					if(cursor!=null && !cursor.isClosed()){
						cursor.close();
					}
				}
			}
		}).start();
	}

	@Override
	public int getCount() {
		if(null != modelData){
			modelData.size();
		}
		return 0;
	}

	@Override
	public Object getData(int location) {
		return modelData.get(location);
	}

	@Override
	public void dispose() {
		super.dispose();
		if(null != modelData){
			modelData.clear();
			modelData = null;
		}
	}
}