#include "sql/operator/join_operator.h"
#include "storage/common/table.h"
#include "rc.h"

JoinOperator::JoinOperator(Operator *left, Operator *right) : left_(left), right_(right)
  {
    rht_it_ = rht_.end();
  }
JoinOperator::~JoinOperator()
{
  for (auto &cpd_rcd : rht_) {
      for (auto rcd : cpd_rcd) {
        delete rcd;
      }
    }
}

RC JoinOperator::open()
{
  //打开连接操作(join)，依次打开左右算子然后调用JoinedTuple的方法进行连接
  RC rc = RC::SUCCESS;
  rc=left_->open();
  rc=right_->open();
  tuple_.init(left_->current_tuple(),right_->current_tuple());
  return rc;
}

RC JoinOperator::fetch_right_table()
{
  RC rc = RC::SUCCESS;
  while (RC::SUCCESS == (rc = right_->next())) {
    // store right records
    CompoundRecord cpd_rcd;
    right_->current_tuple()->get_record(cpd_rcd);
    // need to deep copy the rcd and push back to rht
    // remember to delete them in dtor
    for (auto &rcd_ptr : cpd_rcd) {
      rcd_ptr = new Record(*rcd_ptr);
    }
    rht_.emplace_back(cpd_rcd);
  }
  rht_it_ = rht_.begin();

  if (RC::RECORD_EOF == rc) {
    return RC::SUCCESS;
  }
  return rc;
}

//获取join的下一个元组。此处实现了join操作的笛卡儿积
RC JoinOperator::next()
{
  RC rc = RC::SUCCESS;
  if (is_first_) {
    //首次调用先尝试取左边的元组，对应笛卡尔积从左边开始
    rc = left_->next();
    //以后不会首先取左元组，直到右元组取尽会主动尝试取左元组
    is_first_ = false;
    rc = fetch_right_table();
    if (RC::SUCCESS != rc) {
      return rc;
    }
  }
  if (rht_.end() != rht_it_) {
    CompoundRecord temp(*rht_it_);
    tuple_.set_right_record(temp);
    rht_it_++;
    return RC::SUCCESS;
  }
  // //然后取右元组
  // rc = right_->next();
  // if (RC::SUCCESS == rc) {
  //   return rc;
  // }
  // if (RC::RECORD_EOF != rc) {
  //   //出错
  //   return rc;
  // }
  //右算子的next返回RECORD_EOF表示右算子到达元组末尾，尝试调用左算子的next。这表示笛卡尔积中，一个左元组对应的全部右元组都取完了
  rc = left_->next();
  if (RC::SUCCESS == rc) {
    //如果能成功取到左元组，表示继续笛卡尔积。重置右算子，再取一轮右元组。递归
    // right_->close();
    // right_->open();
    rht_it_ = rht_.begin();
    return next();
  }
  //全部取完了。笛卡尔积结束
  return rc;
}

RC JoinOperator::close()
{
  //关闭连接操作
  left_->close();
  right_->close();
  return RC::SUCCESS;
}

Tuple * JoinOperator::current_tuple()
{
  return &tuple_;
}