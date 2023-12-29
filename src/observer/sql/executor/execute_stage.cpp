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
// Created by Meiyi & Longda on 2021/4/13.
//

#include <string>
#include <sstream>

#include "execute_stage.h"

#include "common/io/io.h"
#include "common/log/log.h"
#include "common/lang/defer.h"
#include "common/seda/timer_stage.h"
#include "common/lang/string.h"
#include "session/session.h"
#include "event/storage_event.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "sql/expr/tuple.h"
#include "sql/operator/table_scan_operator.h"
#include "sql/operator/index_scan_operator.h"
#include "sql/operator/predicate_operator.h"
#include "sql/operator/delete_operator.h"
#include "sql/operator/project_operator.h"
#include "sql/stmt/stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/insert_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/common/table.h"
#include "storage/common/field.h"
#include "storage/index/index.h"
#include "storage/default/default_handler.h"
#include "storage/common/condition_filter.h"
#include "storage/trx/trx.h"
#include "storage/clog/clog.h"

#include "sql/operator/update_operator.h"//add UpdateOperator
#include "sql/operator/join_operator.h"//add JoinOperator for select tables

#include <memory>
#include <vector>
#include "common/defs.h"
#include "sql/expr/expression.h"
#include "sql/operator/operator.h"

using namespace common;

typedef std::vector<FilterUnit *> FilterUnits;

//RC create_selection_executor(
//   Trx *trx, const Selects &selects, const char *db, const char *table_name, SelectExeNode &select_node);

//! Constructor
ExecuteStage::ExecuteStage(const char *tag) : Stage(tag)
{}

//! Destructor
ExecuteStage::~ExecuteStage()
{}

//! Parse properties, instantiate a stage object
Stage *ExecuteStage::make_stage(const std::string &tag)
{
  ExecuteStage *stage = new (std::nothrow) ExecuteStage(tag.c_str());
  if (stage == nullptr) {
    LOG_ERROR("new ExecuteStage failed");
    return nullptr;
  }
  stage->set_properties();
  return stage;
}

//! Set properties for this object set in stage specific properties
bool ExecuteStage::set_properties()
{
  //  std::string stageNameStr(stageName);
  //  std::map<std::string, std::string> section = theGlobalProperties()->get(
  //    stageNameStr);
  //
  //  std::map<std::string, std::string>::iterator it;
  //
  //  std::string key;

  return true;
}

//! Initialize stage params and validate outputs
bool ExecuteStage::initialize()
{
  LOG_TRACE("Enter");

  std::list<Stage *>::iterator stgp = next_stage_list_.begin();
  default_storage_stage_ = *(stgp++);
  mem_storage_stage_ = *(stgp++);

  LOG_TRACE("Exit");
  return true;
}

//! Cleanup after disconnection
void ExecuteStage::cleanup()
{
  LOG_TRACE("Enter");

  LOG_TRACE("Exit");
}

//ltlhasaquestionhere? how to get there. ltlhasaquestion? : how to get there
void ExecuteStage::handle_event(StageEvent *event)
{
  LOG_TRACE("Enter\n");

  handle_request(event);//-> func: handle_request

  LOG_TRACE("Exit\n");
  return;
}

void ExecuteStage::callback_event(StageEvent *event, CallbackContext *context)
{
  LOG_TRACE("Enter\n");

  // here finish read all data from disk or network, but do nothing here.

  LOG_TRACE("Exit\n");
  return;
}

//core func
void ExecuteStage::handle_request(common::StageEvent *event)
{
  SQLStageEvent *sql_event = static_cast<SQLStageEvent *>(event);
  SessionEvent *session_event = sql_event->session_event();
  Stmt *stmt = sql_event->stmt();
  Session *session = session_event->session();
  Query *sql = sql_event->query();

  
  

  if (stmt != nullptr) {
    switch (stmt->type()) {//StmtType
    case StmtType::SELECT: {
      do_select(sql_event);
    } break;
    case StmtType::INSERT: {
      do_insert(sql_event);
    } break;
    case StmtType::UPDATE: {
      //增加update的case，解开注释即可
      do_update((UpdateStmt *)stmt, session_event);
    } break;
    case StmtType::DELETE: {
      do_delete(sql_event);
    } break;
    default: {
      LOG_WARN("should not happen. please implenment");
    } break;
    }
  } else {
    switch (sql->flag) {//SqlCommandFlag
    case SCF_HELP: {
      do_help(sql_event);
    } break;
    case SCF_CREATE_TABLE: {
      do_create_table(sql_event);
    } break;
    case SCF_CREATE_INDEX: {
      do_create_index(sql_event);
    } break;
    case SCF_SHOW_TABLES: {
      do_show_tables(sql_event);
    } break;
    case SCF_DESC_TABLE: {
      do_desc_table(sql_event);
    } break;

    //add case: drop table
    case SCF_DROP_TABLE:{
      do_drop_table(sql_event);
    }break;
    // case SCF_DROP_INDEX:{
    //   do_drop_index(sql_event);
    // }
    case SCF_LOAD_DATA: {
      default_storage_stage_->handle_event(event);
    } break;
    case SCF_SYNC: {
      /*
      RC rc = DefaultHandler::get_default().sync();
      session_event->set_response(strrc(rc));
      */
    } break;
    case SCF_BEGIN: {
      do_begin(sql_event);
      /*
      session_event->set_response("SUCCESS\n");
      */
    } break;
    case SCF_COMMIT: {
      do_commit(sql_event);
      /*
      Trx *trx = session->current_trx();
      RC rc = trx->commit();
      session->set_trx_multi_operation_mode(false);
      session_event->set_response(strrc(rc));
      */
    } break;
    case SCF_CLOG_SYNC: {
      do_clog_sync(sql_event);
    }
    case SCF_ROLLBACK: {
      Trx *trx = session_event->get_client()->session->current_trx();
      RC rc = trx->rollback();
      session->set_trx_multi_operation_mode(false);
      session_event->set_response(strrc(rc));
    } break;
    case SCF_EXIT: {
      // do nothing
      const char *response = "Unsupported\n";
      session_event->set_response(response);
    } break;
    default: {
      LOG_ERROR("Unsupported command=%d\n", sql->flag);
    }
    }
  }
}

void end_trx_if_need(Session *session, Trx *trx, bool all_right)
{
  if (!session->is_trx_multi_operation_mode()) {
    if (all_right) {
      trx->commit();
    } else {
      trx->rollback();
    }
  }
}

void print_tuple_header(std::ostream &os, const ProjectOperator &oper)
{
  const int cell_num = oper.tuple_cell_num();
  const TupleCellSpec *cell_spec = nullptr;
  for (int i = 0; i < cell_num; i++) {
    oper.tuple_cell_spec_at(i, cell_spec);
    if (i != 0) {
      os << " | ";
    }

    if (cell_spec->alias()) {
      os << cell_spec->alias();
    }
  }

  if (cell_num > 0) {
    os << '\n';
  }
}
void tuple_to_string(std::ostream &os, const Tuple &tuple)
{
  TupleCell cell;
  RC rc = RC::SUCCESS;
  bool first_field = true;
  for (int i = 0; i < tuple.cell_num(); i++) {
    rc = tuple.cell_at(i, cell);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to fetch field of cell. index=%d, rc=%s", i, strrc(rc));
      break;
    }

    if (!first_field) {
      os << " | ";
    } else {
      first_field = false;
    }
    cell.to_string(os);
  }
}

IndexScanOperator *try_to_create_index_scan_operator(const FilterUnits &filter_units)
{
  //const std::vector<FilterUnit *> &filter_units = filter_stmt->filter_units();
  if (filter_units.empty() ) {
    return nullptr;
  }

  // 在所有过滤条件中，找到字段与值做比较的条件，然后判断字段是否可以使用索引
  // 如果是多列索引，这里的处理需要更复杂。
  // 这里的查找规则是比较简单的，就是尽量找到使用相等比较的索引
  // 如果没有就找范围比较的，但是直接排除不等比较的索引查询. (你知道为什么?)
  const FilterUnit *better_filter = nullptr;
  for (const FilterUnit * filter_unit : filter_units) {
    if (filter_unit->comp() == NOT_EQUAL) {
      continue;
    }

    Expression *left = filter_unit->left();
    Expression *right = filter_unit->right();
    if (left->type() == ExprType::FIELD && right->type() == ExprType::VALUE) {
    } else if (left->type() == ExprType::VALUE && right->type() == ExprType::FIELD) {
      std::swap(left, right);
    }
    FieldExpr &left_field_expr = *(FieldExpr *)left;
    const Field &field = left_field_expr.field();
    const Table *table = field.table();
    Index *index = table->find_index_by_field(field.field_name());
    if (index != nullptr) {
      if (better_filter == nullptr) {
        better_filter = filter_unit;
      } else if (filter_unit->comp() == EQUAL_TO) {
        better_filter = filter_unit;
    	break;
      }
    }
  }

  if (better_filter == nullptr) {
    return nullptr;
  }

  Expression *left = better_filter->left();
  Expression *right = better_filter->right();
  CompOp comp = better_filter->comp();
  if (left->type() == ExprType::VALUE && right->type() == ExprType::FIELD) {
    std::swap(left, right);
    switch (comp) {
    case EQUAL_TO:    { comp = EQUAL_TO; }    break;
    case LESS_EQUAL:  { comp = GREAT_THAN; }  break;
    case NOT_EQUAL:   { comp = NOT_EQUAL; }   break;
    case LESS_THAN:   { comp = GREAT_EQUAL; } break;
    case GREAT_EQUAL: { comp = LESS_THAN; }   break;
    case GREAT_THAN:  { comp = LESS_EQUAL; }  break;
    default: {
    	LOG_WARN("should not happen");
    }
    }
  }


  FieldExpr &left_field_expr = *(FieldExpr *)left;
  const Field &field = left_field_expr.field();
  const Table *table = field.table();
  Index *index = table->find_index_by_field(field.field_name());
  assert(index != nullptr);

  ValueExpr &right_value_expr = *(ValueExpr *)right;
  TupleCell value;
  right_value_expr.get_tuple_cell(value);

  const TupleCell *left_cell = nullptr;
  const TupleCell *right_cell = nullptr;
  bool left_inclusive = false;
  bool right_inclusive = false;

  switch (comp) {
  case EQUAL_TO: {
    left_cell = &value;
    right_cell = &value;
    left_inclusive = true;
    right_inclusive = true;
  } break;

  case LESS_EQUAL: {
    left_cell = nullptr;
    left_inclusive = false;
    right_cell = &value;
    right_inclusive = true;
  } break;

  case LESS_THAN: {
    left_cell = nullptr;
    left_inclusive = false;
    right_cell = &value;
    right_inclusive = false;
  } break;

  case GREAT_EQUAL: {
    left_cell = &value;
    left_inclusive = true;
    right_cell = nullptr;
    right_inclusive = false;
  } break;

  case GREAT_THAN: {
    left_cell = &value;
    left_inclusive = false;
    right_cell = nullptr;
    right_inclusive = false;
  } break;

  default: {
    LOG_WARN("should not happen. comp=%d", comp);
  } break;
  }

  IndexScanOperator *oper = new IndexScanOperator(table, index,
       left_cell, left_inclusive, right_cell, right_inclusive);

  LOG_INFO("use index for scan: %s in table %s", index->index_meta().name(), table->name());
  return oper;
}

std::unordered_map<Table *, std::unique_ptr<FilterUnits>> split_filters(
    const std::vector<Table *> &tables, FilterStmt *filter_stmt)
{
  std::unordered_map<Table *, std::unique_ptr<FilterUnits>> res;
  for (auto table : tables) {
    res[table] = std::make_unique<FilterUnits>();
  }
  for (auto filter : filter_stmt->filter_units()) {
    Expression *left = filter->left();
    Expression *right = filter->right();
    if (ExprType::FIELD == left->type() && ExprType::VALUE == right->type()) {
    } else if (ExprType::FIELD == right->type() && ExprType::VALUE == left->type()) {
      std::swap(left, right);
    } else {
      continue;
    }
    // TODO: NEED TO CONSIDER SUB_QUERY
    // only support FILED comp VALUE or VALUE comp FILED now
    assert(ExprType::FIELD == left->type() && ExprType::VALUE == right->type());
    auto &left_filed_expr = *static_cast<FieldExpr *>(left);
    const Field &field = left_filed_expr.field();
    res[const_cast<Table *>(field.table())]->emplace_back(filter);
  }
  return res;
}

RC ExecuteStage::do_select(SQLStageEvent *sql_event)
{
  //获取 SelectStmt 对象，表示一个 SELECT 查询语句。
  SelectStmt *select_stmt = (SelectStmt *)(sql_event->stmt());
  //获取与 SQL 事件相关联的会话事件。
  SessionEvent *session_event = sql_event->session_event();
  //定义并初始化一个表示执行结果的变量 rc，初始值为成功
  RC rc = RC::SUCCESS;
  bool flag_multitables=false;
  std::vector<Operator *> delete_opers;
  //检查查询语句涉及的表的数量是否为1，如果不是则输出警告并将 rc 设置为 RC::UNIMPLENMENT，表示不支持多表查询。
  //改：如果表的数目大于1，那么先执行join操作
  /*
  if (select_stmt->tables().size() != 1) {
    LOG_WARN("select more than 1 tables is not supported");
    rc = RC::UNIMPLENMENT;
    return rc;
  }
  //尝试根据查询语句的过滤条件创建一个索引扫描操作符，如果成功则使用该操作符，否则创建一个普通的表扫描操作符。
  Operator *scan_oper = try_to_create_index_scan_operator(select_stmt->filter_stmt());
  if (nullptr == scan_oper) {
    scan_oper = new TableScanOperator(select_stmt->tables()[0]);
  }
  */
  //Operator *scan_oper = new TableScanOperator(select_stmt->tables()[0]);
  Operator *scan_oper=nullptr;
  if (select_stmt->tables().size() > 1){
    //如果表的数目大于1，那么要先执行join操作，生成一个join后的表的扫描算子
    flag_multitables=true;
    rc=join_tables(select_stmt,&scan_oper, delete_opers);
  }else{
    //否则按照原来的方式创建扫描操作符sb
    scan_oper = try_to_create_index_scan_operator(select_stmt->filter_stmt()->filter_units());
    if (nullptr == scan_oper) {
      scan_oper = new TableScanOperator(select_stmt->tables()[0]);
    }
    delete_opers.push_back(scan_oper);
  }
  //使用DEFER 宏，确保在函数返回时释放 scan_oper 对象。
  DEFER([&] () {delete scan_oper;});
  //创建谓词操作符 PredicateOperator，并将表扫描操作符设置为其子操作符。
  PredicateOperator pred_oper(select_stmt->filter_stmt());
  pred_oper.add_child(scan_oper);
  //创建投影操作符 ProjectOperator，将谓词操作符设置为其子操作符，并根据查询语句的字段设置投影。
  ProjectOperator project_oper;
  project_oper.add_child(&pred_oper);
  //TODO 设置投影 改：
  for (const Field &field : select_stmt->query_fields()) {
    project_oper.add_projection(field.table(),field.meta(),flag_multitables);
  }
  //打开投影操作符，初始化执行
  rc = project_oper.open();
  //如果打开操作符失败，则输出警告并返回错误码
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open operator");
    return rc;
  }
  //创建一个 std::stringstream 对象 ss，用于构建查询结果的字符串表示。
  std::stringstream ss;
  //将投影操作符的元组头信息输出到字符串流中。
  print_tuple_header(ss, project_oper);
  //迭代执行投影操作符，将查询结果的元组转换为字符串并添加到字符串流中。
  while ((rc = project_oper.next()) == RC::SUCCESS) {
    // get current record
    // write to response
    Tuple * tuple = project_oper.current_tuple();
    if (nullptr == tuple) {
      rc = RC::INTERNAL;
      LOG_WARN("failed to get current record. rc=%s", strrc(rc));
      break;
    }

    tuple_to_string(ss, *tuple);
    ss << std::endl;
  }
  //如果迭代操作符时出现错误而不是到达记录末尾，则输出警告并关闭操作符，否则只关闭操作符。
  if (rc != RC::RECORD_EOF) {
    LOG_WARN("something wrong while iterate operator. rc=%s", strrc(rc));
    project_oper.close();
  } else {
    rc = project_oper.close();
  }
  //将构建的查询结果字符串设置为会话事件的响应内容。
  session_event->set_response(ss.str());
  //返回执行结果码
  return rc;
}

RC ExecuteStage::do_help(SQLStageEvent *sql_event)
{
  SessionEvent *session_event = sql_event->session_event();
  const char *response = "show tables;\n"
                         "desc `table name`;\n"
                         "create table `table name` (`column name` `column type`, ...);\n"
                         "create index `index name` on `table` (`column`);\n"
                         "insert into `table` values(`value1`,`value2`);\n"
                         "update `table` set column=value [where `column`=`value`];\n"
                         "delete from `table` [where `column`=`value`];\n"
                         "select [ * | `columns` ] from `table`;\n";
  session_event->set_response(response);
  return RC::SUCCESS;
}

RC ExecuteStage::do_create_table(SQLStageEvent *sql_event)
{
  /*
  //struct of craete_table (sstr.create_table)
    name of table, count of columns, info of columns
    typedef struct {
      char *relation_name;           // Relation name
      size_t attribute_count;        // Length of attribute
      AttrInfo attributes[MAX_NUM];  // attributes
    } CreateTable;
  */
  //get createtable object
  const CreateTable &create_table = sql_event->query()->sstr.create_table;
  //following two statements are a whole: connecting db
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  //call function of creating table, parameter is the createtable object
  //-> ../storege/common/db.cpp func: create_table
  RC rc = db->create_table(create_table.relation_name,
			create_table.attribute_count, create_table.attributes);
  if (rc == RC::SUCCESS) {
    session_event->set_response("SUCCESS\n");
  } else {
    session_event->set_response("FAILURE\n");
  }
  return rc;
}
RC ExecuteStage::do_create_index(SQLStageEvent *sql_event)
{
  /*
  //struct of create_index
    typedef struct {
      char *index_name;      // Index name
      char *relation_name;   // Relation name
      char *attribute_name;  // Attribute name
    } CreateIndex;
  */
  //following two statements are a whole: connecting db
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  //get createindex object
  const CreateIndex &create_index = sql_event->query()->sstr.create_index;
  //find corresponding table
  Table *table = db->find_table(create_index.relation_name);
  if (nullptr == table) {
    session_event->set_response("FAILURE\n");
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  //call function of creating index
  RC rc = table->create_index(nullptr, create_index.index_name, create_index.attribute_name);
  sql_event->session_event()->set_response(rc == RC::SUCCESS ? "SUCCESS\n" : "FAILURE\n");
  return rc;
}

RC ExecuteStage::do_show_tables(SQLStageEvent *sql_event)
{
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  std::vector<std::string> all_tables;
  db->all_tables(all_tables);
  if (all_tables.empty()) {
    session_event->set_response("No table\n");
  } else {
    std::stringstream ss;
    for (const auto &table : all_tables) {
      ss << table << std::endl;
    }
    session_event->set_response(ss.str().c_str());
  }
  return RC::SUCCESS;
}

RC ExecuteStage::do_desc_table(SQLStageEvent *sql_event)
{
  Query *query = sql_event->query();
  Db *db = sql_event->session_event()->session()->get_current_db();
  const char *table_name = query->sstr.desc_table.relation_name;
  Table *table = db->find_table(table_name);
  std::stringstream ss;
  if (table != nullptr) {
    table->table_meta().desc(ss);
  } else {
    ss << "No such table: " << table_name << std::endl;
  }
  sql_event->session_event()->set_response(ss.str().c_str());
  return RC::SUCCESS;
}

RC ExecuteStage::do_insert(SQLStageEvent *sql_event)
{
  Stmt *stmt = sql_event->stmt();
  SessionEvent *session_event = sql_event->session_event();
  Session *session = session_event->session();
  Db *db = session->get_current_db();
  Trx *trx = session->current_trx();
  CLogManager *clog_manager = db->get_clog_manager();

  if (stmt == nullptr) {
    LOG_WARN("cannot find statement");
    return RC::GENERIC_ERROR;
  }

  InsertStmt *insert_stmt = (InsertStmt *)stmt;
  Table *table = insert_stmt->table();

  RC rc = table->insert_record(trx, insert_stmt->value_amount(), insert_stmt->values());
  if (rc == RC::SUCCESS) {
    if (!session->is_trx_multi_operation_mode()) {
      CLogRecord *clog_record = nullptr;
      rc = clog_manager->clog_gen_record(CLogType::REDO_MTR_COMMIT, trx->get_current_id(), clog_record);
      if (rc != RC::SUCCESS || clog_record == nullptr) {
        session_event->set_response("FAILURE\n");
        return rc;
      }

      rc = clog_manager->clog_append_record(clog_record);
      if (rc != RC::SUCCESS) {
        session_event->set_response("FAILURE\n");
        return rc;
      } 

      trx->next_current_id();
      session_event->set_response("SUCCESS\n");
    } else {
      session_event->set_response("SUCCESS\n");
    }
  } else {
    session_event->set_response("FAILURE\n");
  }
  return rc;
}

RC ExecuteStage::do_delete(SQLStageEvent *sql_event)
{
  Stmt *stmt = sql_event->stmt();
  SessionEvent *session_event = sql_event->session_event();
  Session *session = session_event->session();
  Db *db = session->get_current_db();
  Trx *trx = session->current_trx();
  CLogManager *clog_manager = db->get_clog_manager();

  if (stmt == nullptr) {
    LOG_WARN("cannot find statement");
    return RC::GENERIC_ERROR;
  }

  DeleteStmt *delete_stmt = (DeleteStmt *)stmt;
  TableScanOperator scan_oper(delete_stmt->table());
  PredicateOperator pred_oper(delete_stmt->filter_stmt());
  pred_oper.add_child(&scan_oper);
  DeleteOperator delete_oper(delete_stmt, trx);
  delete_oper.add_child(&pred_oper);

  RC rc = delete_oper.open();
  if (rc != RC::SUCCESS) {
    session_event->set_response("FAILURE\n");
  } else {
    session_event->set_response("SUCCESS\n");
    if (!session->is_trx_multi_operation_mode()) {
      CLogRecord *clog_record = nullptr;
      rc = clog_manager->clog_gen_record(CLogType::REDO_MTR_COMMIT, trx->get_current_id(), clog_record);
      if (rc != RC::SUCCESS || clog_record == nullptr) {
        session_event->set_response("FAILURE\n");
        return rc;
      }

      rc = clog_manager->clog_append_record(clog_record);
      if (rc != RC::SUCCESS) {
        session_event->set_response("FAILURE\n");
        return rc;
      } 

      trx->next_current_id();
      session_event->set_response("SUCCESS\n");
    }
  }
  return rc;
}

RC ExecuteStage::do_begin(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;
  SessionEvent *session_event = sql_event->session_event();
  Session *session = session_event->session();
  Db *db = session->get_current_db();
  Trx *trx = session->current_trx();
  CLogManager *clog_manager = db->get_clog_manager();

  session->set_trx_multi_operation_mode(true);

  CLogRecord *clog_record = nullptr;
  rc = clog_manager->clog_gen_record(CLogType::REDO_MTR_BEGIN, trx->get_current_id(), clog_record);
  if (rc != RC::SUCCESS || clog_record == nullptr) {
    session_event->set_response("FAILURE\n");
    return rc;
  }

  rc = clog_manager->clog_append_record(clog_record);
  if (rc != RC::SUCCESS) {
    session_event->set_response("FAILURE\n");
  } else {
    session_event->set_response("SUCCESS\n");
  }

  return rc;
}

RC ExecuteStage::do_commit(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;
  SessionEvent *session_event = sql_event->session_event();
  Session *session = session_event->session();
  Db *db = session->get_current_db();
  Trx *trx = session->current_trx();
  CLogManager *clog_manager = db->get_clog_manager();

  session->set_trx_multi_operation_mode(false);

  CLogRecord *clog_record = nullptr;
  rc = clog_manager->clog_gen_record(CLogType::REDO_MTR_COMMIT, trx->get_current_id(), clog_record);
  if (rc != RC::SUCCESS || clog_record == nullptr) {
    session_event->set_response("FAILURE\n");
    return rc;
  }

  rc = clog_manager->clog_append_record(clog_record);
  if (rc != RC::SUCCESS) {
    session_event->set_response("FAILURE\n");
  } else {
    session_event->set_response("SUCCESS\n");
  }

  trx->next_current_id();

  return rc;
}

RC ExecuteStage::do_clog_sync(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  CLogManager *clog_manager = db->get_clog_manager();

  rc = clog_manager->clog_sync();
  if (rc != RC::SUCCESS) {
    session_event->set_response("FAILURE\n");
  } else {
    session_event->set_response("SUCCESS\n");
  }

  return rc;
}

RC ExecuteStage::do_drop_table(SQLStageEvent *sql_event){
  /*
  struct of drop_table
  typedef struct {
    char *relation_name;  // Relation name
  } DropTable;
  */
  //get drop table object
  const DropTable &drop_table=sql_event->query()->sstr.drop_table; 
  //connecting db
  SessionEvent *session_event=sql_event->session_event();
  Db *db=session_event->session()->get_current_db();
  //call function of droping table
  RC rc=db->drop_table(drop_table.relation_name);
  if(rc==RC::SUCCESS){
    session_event->set_response("SUCCESS\n");
  } else{
    session_event->set_response("FAILURE\n");
  }
  return rc;
}

// RC ExecuteStage::do_drop_index(SQLStageEvent *sql_event){
//   /*
//   //struct of  drop_index
//     typedef struct {
//       const char *index_name;  // Index name
//     } DropIndex;
//   */
//   //connecting db
//   SessionEvent *session_event=sql_event->session_event();
//   Db *db=session_event->session()->get_current_db();
//   //get dropindex object
//   const DropIndex &drop_index=sql_event->query()->sstr.drop_index;
//   //find corresponding table
//   Table *table=db->find_table(drop_index.relation_name);
//   //call function of droping index
//   RC rc=table->drop_index();
// }

//完全仿照do_delete函数即可，但需要先实现UpdateOperator，实现方法也仿照deleteoperator和select
RC ExecuteStage::do_update(UpdateStmt *stmt,SessionEvent *session_event){
  //检查是否是空对象
  if (stmt == nullptr) {
    LOG_WARN("cannot find statement");
    return RC::GENERIC_ERROR;
  }
  //以下是一个复用性很强的过程，delete、select等过程中都用以下过程，直接套用
  TableScanOperator scan_oper(stmt->table());//表扫描操作对象，扫描记录
  PredicateOperator pred_oper(stmt->filter_stmt());//对记录进行过滤，对象的.next()方法对记录进行过滤，找到满足条件的记录
  pred_oper.add_child(&scan_oper);//pred_oper对象的方法总是调用第一个孩子的方法，所以要添加children
  UpdateOperator update_oper(stmt);//同上
  update_oper.add_child(&pred_oper);//同上，调用孩子的方法
  //将表扫描操作符添加为谓词操作符的子操作符，将谓词操作符添加为删除操作符的子操作符，形成操作符之间的关联关系。
  //完成了一个类似树数据结构的调用过程：UpdateOperator -> PredicateOperator -> TableScanOperator
  //执行过程反过来，从树的叶子节点往上
  //因此，构建执行过程的方法很简单，重点是得实现UpdateOperator和table.cpp中更新记录方法

  //执行更新
  RC rc = update_oper.open();
  if (rc != RC::SUCCESS) {
    session_event->set_response("FAILURE\n");
  } else {
    //成功执行，此处有针对事务的处理。可以不做，因为暂时没有需求
    session_event->set_response("SUCCESS\n");
  }
  return rc;
}

/*
//构造一个join算子的对象
RC ExecuteStage::join_tables(SelectStmt *select_stmt, Operator **joined_scan_oper){
  //创建存储算子的动态数组，用来迭代产生最终的join后的表扫描算子
  std::vector<Operator *> operators;
  //便利查询语句中的每个表，如果有索引则利用索引扫描算子，否则使用普通的表扫描算子，将扫描算子添加到动态数组中
  for (size_t i=0; i<select_stmt->tables().size();i++){
    Operator *scan_oper=try_to_create_index_scan_operator(select_stmt->filter_stmt());
    if (scan_oper==nullptr) {
      scan_oper=new TableScanOperator(select_stmt->tables()[i]);
    }
    operators.push_back(scan_oper);
  }
  //迭代，将动态数组中的扫描算子两两合并（join）成join操作符。直到最后只剩下一个算子，它就是最终的join后的表的扫描算子
  while (operators.size() > 1) {
    Operator* left_oper=operators.back();
    operators.pop_back();
    Operator* right_oper=operators.back();
    operators.pop_back();
    //生成join算子，详细实现见此类
    JoinOperator* join_oper=new JoinOperator(left_oper, right_oper);
    operators.push_back(join_oper);
  }
  //将它保存到传入的指针中
  *joined_scan_oper=operators.back();
  return RC::SUCCESS;
}
*/

RC ExecuteStage::join_tables(SelectStmt *select_stmt, Operator **joined_scan_oper, std::vector<Operator *> &delete_opers){
  const auto &tables = select_stmt->tables();
  FilterStmt *filter_stmt = select_stmt->filter_stmt();
  auto table_filters_ht = split_filters(tables, filter_stmt);
  //创建存储算子的动态数组，用来迭代产生最终的join后的表扫描算子
  std::vector<Operator *> operators;
  //便利查询语句中的每个表，如果有索引则利用索引扫描算子，否则使用普通的表扫描算子，将扫描算子添加到动态数组中
  for (std::vector<Table *>::size_type i = 0; i < tables.size(); i++){
    Operator *scan_oper=try_to_create_index_scan_operator(*table_filters_ht[tables[i]]);
    if (scan_oper==nullptr) {
      scan_oper=new TableScanOperator(tables[i]);
    }
    delete_opers.push_back(scan_oper);
    operators.push_back(scan_oper);
  }
  //迭代，将动态数组中的扫描算子两两合并（join）成join操作符。直到最后只剩下一个算子，它就是最终的join后的表的扫描算子
  while (operators.size() > 1) {
    Operator* left_oper=operators.back();
    operators.pop_back();
    Operator* right_oper=operators.back();
    operators.pop_back();
    //生成join算子，详细实现见此类
    JoinOperator* join_oper=new JoinOperator(left_oper, right_oper);
    operators.push_back(join_oper);
  }
  //将它保存到传入的指针中
  *joined_scan_oper=operators.back();
  return RC::SUCCESS;
}