/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#pragma once

#include "rc.h"
#include "sql/stmt/stmt.h"

//需要首先完善update_stmt，update语句创建、updateoperator等都依赖于此类
//仿照select_stmt和delete_stmt

class Table;
class FilterStmt;//add filter

class UpdateStmt : public Stmt
{
public:

  UpdateStmt() = default;
  //UpdateStmt(Table *table, Value *values, int value_amount);
  
  UpdateStmt(Table *table,char *field_name,Value *values,FilterStmt *filter_stmt);//完善update_stmt
  ~UpdateStmt() override;//完善update_stmt
  StmtType type() const override { return StmtType::UPDATE; }//完善update_stmt,返回语句类型

public:
  static RC create(Db *db, const Updates &update_sql, Stmt *&stmt);//根据sql指令创建update语句对象

public:
  Table *table() const {return table_;}//返回对应表
  Value *values() const { return values_; }
  int value_amount() const { return value_amount_; }

  char *field_name() const {return field_name_;}//add field name
  FilterStmt *filter_stmt() const {return filter_stmt_;}//add filter

private:
  Table *table_ = nullptr;
  Value *values_ = nullptr;
  int value_amount_ = 0;

  char *field_name_ = nullptr;//add field name
  FilterStmt *filter_stmt_ = nullptr;//add filter
};

