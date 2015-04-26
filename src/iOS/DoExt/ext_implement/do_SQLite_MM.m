//
//  do_SQLite_MM.m
//  DoExt_MM
//
//  Created by @userName on @time.
//  Copyright (c) 2015年 DoExt. All rights reserved.
//

#import "do_SQLite_MM.h"

#import "doScriptEngineHelper.h"
#import "doIScriptEngine.h"
#import "doInvokeResult.h"
#import <sqlite3.h>
#import "doJsonNode.h"
#import "doIScriptEngine.h"
#import "doIApp.h"
#import "doIDataFS.h"

@implementation do_SQLite_MM
{
    @private
    sqlite3 *dbConnection;
}
#pragma mark - 注册属性（--属性定义--）
/*
 [self RegistProperty:[[doProperty alloc]init:@"属性名" :属性类型 :@"默认值" : BOOL:是否支持代码修改属性]];
 */
-(void)OnInit
{
    [super OnInit];
    //注册属性
}

//销毁所有的全局对象
-(void)Dispose
{
    sqlite3_close(dbConnection);
    dbConnection = nil;
}
#pragma mark -
#pragma mark - 同步异步方法的实现
/*
 1.参数节点
 doJsonNode *_dictParas = [parms objectAtIndex:0];
 a.在节点中，获取对应的参数
 NSString *title = [_dictParas GetOneText:@"title" :@"" ];
 说明：第一个参数为对象名，第二为默认值
 
 2.脚本运行时的引擎
 id<doIScriptEngine> _scritEngine = [parms objectAtIndex:1];
 
 同步：
 3.同步回调对象(有回调需要添加如下代码)
 doInvokeResult *_invokeResult = [parms objectAtIndex:2];
 回调信息
 如：（回调一个字符串信息）
 [_invokeResult SetResultText:((doUIModule *)_model).UniqueKey];
 异步：
 3.获取回调函数名(异步方法都有回调)
 NSString *_callbackName = [parms objectAtIndex:2];
 在合适的地方进行下面的代码，完成回调
 新建一个回调对象
 doInvokeResult *_invokeResult = [[doInvokeResult alloc] init];
 填入对应的信息
 如：（回调一个字符串）
 [_invokeResult SetResultText: @"异步方法完成"];
 [_scritEngine Callback:_callbackName :_invokeResult];
 */
//同步
 - (void)close:(NSArray *)parms
 {
     [self Dispose];
     //自己的代码实现
 }
 - (void)open:(NSArray *)parms
 {
     doJsonNode *_dictParas = [parms objectAtIndex:0];
     id<doIScriptEngine> _scritEngine = [parms objectAtIndex:1];
     doInvokeResult *_invokeResult = [parms objectAtIndex:2];
     
     NSString* dbPath = [_dictParas GetOneText:@"path" :@":memory:" ];
     NSString* dbName = dbPath;
     //只支持:memory: 或者data://打头
     if(![dbPath isEqualToString:@":memory:"])
     {
         dbName = [_scritEngine.CurrentApp.DataFS GetFileFullPathByName:dbPath];
     }
     int result = sqlite3_open([dbName UTF8String], &dbConnection);
     if (result == SQLITE_OK) {
         // 创建打开成功
         [_invokeResult SetResultBoolean:YES];
     }else{
         //创建或者打开失败
         [_invokeResult SetResultBoolean:NO];
     }
 }
//异步
- (void)execute:(NSArray *)parms
{
    doJsonNode *_dictParas = [parms objectAtIndex:0];
    id<doIScriptEngine> _scritEngine = [parms objectAtIndex:1];
    //自己的代码实现
    NSString* sqlString = [_dictParas GetOneText:@"sql" :@"" ];
    int result = sqlite3_exec(dbConnection, [sqlString UTF8String], NULL, NULL, NULL);
    doInvokeResult *_invokeResult = [[doInvokeResult alloc] init];
    if (result == SQLITE_OK) {
        //sql 执行成功
        [_invokeResult SetResultBoolean:YES];
    }else{
        //sql 执行失败
        [_invokeResult SetResultBoolean:NO];
    }
    NSString *_callbackName = [parms objectAtIndex:2];
    [_scritEngine Callback:_callbackName :_invokeResult];
}
- (void)query:(NSArray *)parms
{
    doJsonNode *_dictParas = [parms objectAtIndex:0];
    id<doIScriptEngine> _scritEngine = [parms objectAtIndex:1];
    doInvokeResult *_invokeResult = [[doInvokeResult alloc] init];

    //自己的代码实现
    NSString* sqlString = [_dictParas GetOneText:@"sql" :@"" ];
    sqlite3_stmt *statement;
    doJsonNode * node = [[doJsonNode alloc]init];
    @try {
        int result = sqlite3_prepare_v2(dbConnection, [sqlString UTF8String], -1, &statement, nil);
        if (result == SQLITE_OK) {
            //查询成功
            int columnCount = sqlite3_column_count(statement);
            while (sqlite3_step(statement) == SQLITE_ROW)
            {
                for(int i = 0;i<columnCount;i++){
                    const char *_columnName=sqlite3_column_name(statement, i);
                    NSString *columnName=[[NSString alloc] initWithUTF8String:_columnName];
                    
                    char *_rowData = (char *)sqlite3_column_text(statement, i);
                    NSString *rowData = [[NSString alloc] initWithUTF8String:_rowData];
                    [node SetOneText:columnName :rowData];
                }
            }
        }
        else{
            [NSException raise:@"doSqlite" format:@"sql:%@无效!",sqlString,nil];
        }
        [_invokeResult SetResultNode:node];
    }
    @catch (NSException *exception) {
        [_invokeResult SetException:exception];
    }
    @finally {
        sqlite3_finalize(statement);
    }
    
    NSString *_callbackName = [parms objectAtIndex:2];
    [_scritEngine Callback:_callbackName :_invokeResult];
}

@end