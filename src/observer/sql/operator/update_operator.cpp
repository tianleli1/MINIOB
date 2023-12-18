#include "common/log/log.h"
#include "sql/operator/update_operator.h"
#include "storage/record/record.h"
#include "storage/common/table.h"
//#include "storage/trx/trx.h"
#include "sql/stmt/update_stmt.h"

//Update的重点之一，实现更新算子
//几乎直接模仿DeleteOperator就行，不需要trx，因为暂时没有事务相关需求
UpdateOperator::UpdateOperator(UpdateStmt *update_stmt) : update_stmt_(update_stmt){}
UpdateOperator::~UpdateOperator(){}
RC UpdateOperator::open()
{
  if (children_.size() != 1) {
    LOG_WARN("update operator must has 1 child");
    return RC::INTERNAL;
  }

  Operator *child = children_[0];
  RC rc = child->open();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  Table *table = update_stmt_->table();
  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record &record = row_tuple->record();
    //rc = table->update_record(nullptr, &record);
    //use update_record

    //以上与deleteoperator完全一致，但是接下来要调用table->update_record。只有此处要修改
    auto attr_name = update_stmt_->field_name();
    auto value = update_stmt_->values();
    rc = table->update_record(nullptr, &record, attr_name, value);


    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record: %s", strrc(rc));
      return rc;
    }
  }
  return RC::SUCCESS;
}

RC UpdateOperator::next()
{
  return RC::RECORD_EOF;
}

RC UpdateOperator::close()
{
  children_[0]->close();
  return RC::SUCCESS;
}