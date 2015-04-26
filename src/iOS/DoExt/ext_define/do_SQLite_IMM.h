//
//  do_SQLite_MM.h
//  DoExt_MM
//
//  Created by @userName on @time.
//  Copyright (c) 2015年 DoExt. All rights reserved.
//

#import <Foundation/Foundation.h>

@protocol do_SQLite_IMM <NSObject>
//实现同步或异步方法，parms中包含了所需用的属性
- (void)close:(NSArray *)parms;
- (void)execute:(NSArray *)parms;
- (void)open:(NSArray *)parms;
- (void)query:(NSArray *)parms;

@end