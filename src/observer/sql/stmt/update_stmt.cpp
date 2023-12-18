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


#include "sql/stmt/update_stmt.h"

#include "common/log/log.h"
#include "storage/common/db.h"
#include "storage/common/table.h"
#include "sql/stmt/filter_stmt.h"

//完善update_stmt,仿照select_stmt和delete_stmt
UpdateStmt::UpdateStmt(Table *table,char *field_name,Value *values,FilterStmt *filter_stmt)
  : table_ (table), field_name_(field_name), values_(values),filter_stmt_(filter_stmt)
{}

UpdateStmt::~UpdateStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  } 
}

RC UpdateStmt::create(Db *db, const Updates &update_sql, Stmt *&stmt)
{
  const char *table_name = update_sql.relation_name;
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", 
             db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  //检查要更新的表和字段是否合法，即数据库中是否存在对应的表和字段。
  // check whether the table exists: same as delete_stmt.cpp
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table");
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // check whether the field exists: same as select_stmt.cpp
  const char *field_name = update_sql.attribute_name;
  const FieldMeta *field_meta = table->table_meta().field(field_name);
  if (nullptr == field_meta) {
      LOG_WARN("no such field");
      return RC::SCHEMA_FIELD_MISSING;
  }

  // get filter_stmt, same as delete_stmt.cpp
  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));

  FilterStmt *filter_stmt = nullptr;
  RC rc = FilterStmt::create(db, table, &table_map,
			     update_sql.conditions, update_sql.condition_num, filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  // get values
  Value *values=const_cast<Value*>(&update_sql.value);
  // create update stmt
  stmt = new UpdateStmt(table,(char*)field_name,values,filter_stmt);
  return rc;
}
