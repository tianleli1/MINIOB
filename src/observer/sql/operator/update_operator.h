#pragma once

#include "sql/operator/operator.h"
#include "rc.h"

//Update的重点之一，实现更新算子
//几乎直接模仿DeleteOperator就行，不需要trx，因为暂时没有事务相关需求

//class Trx;
class UpdateStmt;

class UpdateOperator : public Operator
{
public:
  //UpdateOperator(UpdateStmt *update_stmt, Trx *trx) : update_stmt_(update_stmt), trx_(trx){}
  //UpdateOperator(UpdateStmt *update_stmt) : update_stmt_(update_stmt){}
  UpdateOperator(UpdateStmt *update_stmt);
  //~UpdateOperator() = default;
  ~UpdateOperator();

  RC open() override;
  RC next() override;
  RC close() override;

  Tuple * current_tuple() override {
    return nullptr;
  }
  //int tuple_cell_num() const override
  //RC tuple_cell_spec_at(int index, TupleCellSpec &spec) const override
private:
  UpdateStmt *update_stmt_ = nullptr;
  //Trx *trx_ = nullptr;
};